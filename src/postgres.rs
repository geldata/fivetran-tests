use std::net::SocketAddr;

use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::Row;

pub async fn validate_data(addr: SocketAddr) -> anyhow::Result<()> {
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let (client, conn) = tokio_postgres::Config::new()
        .host("localhost")
        .port(addr.port())
        .user("username")
        .password("pass")
        .dbname("postgres")
        .ssl_mode(tokio_postgres::config::SslMode::Prefer)
        .connect(connector)
        .await?;

    tokio::task::spawn(async {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e);
        }
    });

    test_tables(&client).await?;

    Ok(())
}

async fn query_to_text(client: &tokio_postgres::Client, query: &str) -> anyhow::Result<String> {
    let rows = client.query(query, &[]).await?;
    Ok(result_to_text(rows))
}

fn result_to_text(rows: Vec<Row>) -> String {
    let mut r = String::new();

    // header
    if let Some(row) = rows.first() {
        for (i, c) in row.columns().iter().enumerate() {
            if i > 0 {
                r += ", ";
            }
            r += c.name();
        }
        r += "\n";
    } else {
        r += "<empty>";
    }

    // data
    for row in rows {
        for (i, _) in row.columns().iter().enumerate() {
            if i > 0 {
                r += ", ";
            }
            if let Some(s) = row.get::<_, Option<&str>>(i) {
                r += s;
            } else {
                r += "NULL";
            }
        }
        r += "\n";
    }
    r
}

fn assert_eq(found: String, expected: &'static str) -> anyhow::Result<()> {
    if expected.trim() == found.trim() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("found:\n{found}\nexpected:\n{expected}"))
    }
}

async fn test_tables(c: &tokio_postgres::Client) -> anyhow::Result<()> {
    assert_eq(
        query_to_text(
            c,
            r#"
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name"#,
        )
        .await?,
        r#"
table_schema, table_name
gel_public, book
gel_public, book_chapters
gel_public, content
gel_public, contentsummary
gel_public, genre
gel_public, movie
gel_public, movie_actors
gel_public, movie_director
gel_public, novel
gel_public, novel_chapters
gel_public, person
gel_public___links, a
gel_public___links, b
gel_public___links, b_a
gel_public___links, b_prop
gel_public___links, b_vals
gel_public___links, c
gel_public___links, c_a
gel_public___links, c_prop
gel_public___links, c_vals
gel_public___nested, hello
gel_public___nested___deep, rolling
        "#,
    )?;

    assert_eq(
        query_to_text(
            c,
            r#"
            SELECT table_schema, table_name, column_name
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
              AND column_name NOT LIKE '_fivetran_%'
            ORDER BY table_schema, table_name, ordinal_position"#,
        )
        .await?,
        r#"
table_schema, table_name, column_name
gel_public, book, id
gel_public, book, __type__
gel_public, book, genre_id
gel_public, book, pages
gel_public, book, title
gel_public, book_chapters, source
gel_public, book_chapters, target
gel_public, content, id
gel_public, content, __type__
gel_public, content, genre_id
gel_public, content, title
gel_public, contentsummary, id
gel_public, contentsummary, __type__
gel_public, genre, id
gel_public, genre, __type__
gel_public, genre, name
gel_public, movie, id
gel_public, movie, __type__
gel_public, movie, director_id
gel_public, movie, genre_id
gel_public, movie, release_year
gel_public, movie, title
gel_public, movie_actors, source
gel_public, movie_actors, target
gel_public, movie_actors, role
gel_public, movie_director, source
gel_public, movie_director, target
gel_public, movie_director, bar
gel_public, novel, id
gel_public, novel, __type__
gel_public, novel, foo
gel_public, novel, genre_id
gel_public, novel, pages
gel_public, novel, title
gel_public, novel_chapters, source
gel_public, novel_chapters, target
gel_public, person, id
gel_public, person, __type__
gel_public, person, first_name
gel_public, person, last_name
gel_public___links, a, id
gel_public___links, a, __type__
gel_public___links, b, id
gel_public___links, b, __type__
gel_public___links, b, prop_id
gel_public___links, b_a, source
gel_public___links, b_a, target
gel_public___links, b_prop, source
gel_public___links, b_prop, target
gel_public___links, b_prop, lp
gel_public___links, b_vals, source
gel_public___links, b_vals, target
gel_public___links, c, id
gel_public___links, c, __type__
gel_public___links, c, prop_id
gel_public___links, c_a, source
gel_public___links, c_a, target
gel_public___links, c_prop, source
gel_public___links, c_prop, target
gel_public___links, c_prop, lp
gel_public___links, c_vals, source
gel_public___links, c_vals, target
gel_public___nested, hello, id
gel_public___nested, hello, __type__
gel_public___nested, hello, hello
gel_public___nested___deep, rolling, id
gel_public___nested___deep, rolling, __type__
gel_public___nested___deep, rolling, rolling
        "#,
    )?;

    assert_eq(
        query_to_text(c, r#"SELECT name FROM gel_public.genre ORDER BY name"#).await?,
        r#"
name
Drama
Fiction
武侠
        "#,
    )?;

    assert_eq(
        query_to_text(
            c,
            r#"
        SELECT first_name, last_name
        FROM gel_public.person
        ORDER BY first_name"#,
        )
        .await?,
        r#"
first_name, last_name
Robin, NULL
Steven, Spielberg
Tom, Hanks
        "#,
    )?;

    assert_eq(
        query_to_text(
            c,
            r#"
        SELECT title, release_year::text, d.first_name as director, g.name as genre
        FROM gel_public.movie m
        LEFT JOIN gel_public.genre g on (g.id = m.genre_id)
        LEFT JOIN gel_public.person d on (d.id = m.director_id)
        ORDER BY title"#,
        )
        .await?,
        r#"
title, release_year, director, genre
Forrest Gump, 1994, NULL, Drama
Saving Private Ryan, 1998, Steven, Drama
        "#,
    )?;

    assert_eq(
        query_to_text(
            c,
            r#"
        SELECT m.title, ma.role, a.first_name
        FROM gel_public.movie_actors ma
        LEFT JOIN gel_public.movie m on (m.id = ma.source)
        LEFT JOIN gel_public.person a on (a.id = ma.target)
        ORDER BY m.title, a.first_name
        "#,
        )
        .await?,
        r#"
title, role, first_name
Forrest Gump, NULL, Robin
Forrest Gump, NULL, Tom
Saving Private Ryan, Captain Miller, Tom
        "#,
    )?;

    assert_eq(
        query_to_text(
            c,
            r#"
        SELECT c.title, g.name as genre
        FROM ONLY gel_public.content c
        LEFT JOIN gel_public.genre g on (g.id = c.genre_id)
        ORDER BY c.title
        "#,
        )
        .await?,
        r#"
title, genre
Chronicles of Narnia, Fiction
Forrest Gump, Drama
Halo 3, Fiction
Hunger Games, Fiction
Saving Private Ryan, Drama
        "#,
    )?;

    assert_eq(
        query_to_text(
            c,
            r#"
        SELECT b.title, b.pages::text, g.name as genre
        FROM ONLY gel_public.book b
        LEFT JOIN gel_public.genre g on (g.id = b.genre_id)
        ORDER BY b.title
        "#,
        )
        .await?,
        r#"
title, pages, genre
Chronicles of Narnia, 206, Fiction
Hunger Games, 374, Fiction
        "#,
    )?;

    assert_eq(
        query_to_text(
            c,
            r#"
        SELECT b.title, bc.target as chapter
        FROM ONLY gel_public.book_chapters bc
        LEFT JOIN gel_public.book b on (b.id = bc.source)
        ORDER BY b.title, bc.target
        "#,
        )
        .await?,
        r#"
title, chapter
Chronicles of Narnia, Edmund and the wardrobe
Chronicles of Narnia, Lucy looks into a wardrobe
Chronicles of Narnia, Turkish delight
Chronicles of Narnia, What Lucy found there
Hunger Games, Part 1
Hunger Games, Part 2
Hunger Games, Part 3
        "#,
    )?;

    assert_eq(
        query_to_text(
            c,
            r#"
        SELECT n.title, n.pages::text, g.name as genre
        FROM ONLY gel_public.novel n
        LEFT JOIN gel_public.genre g on (g.id = n.genre_id)
        ORDER BY n.title
        "#,
        )
        .await?,
        r#"
title, pages, genre
Hunger Games, 374, Fiction
        "#,
    )?;

    assert_eq(
        query_to_text(
            c,
            r#"
        SELECT n.title, nc.target as chapter
        FROM ONLY gel_public.novel_chapters nc
        LEFT JOIN gel_public.novel n on (n.id = nc.source)
        ORDER BY n.title, nc.target
        "#,
        )
        .await?,
        r#"
title, chapter
Hunger Games, Part 1
Hunger Games, Part 2
Hunger Games, Part 3
        "#,
    )?;

    Ok(())
}
