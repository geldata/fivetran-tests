
insert Genre { name:= 'Fiction' };
insert Genre { name:= 'Drama' };
insert Genre { name:= '武侠' };

insert Person { first_name:= 'Tom', last_name:= 'Hanks' };
insert Person { first_name:= 'Robin' };
insert Person { first_name:= 'Steven', last_name:= 'Spielberg' };

insert Movie {
    title := 'Forrest Gump',
    release_year := 1994,
    actors := (select Person
        filter .first_name = 'Tom' or .first_name = 'Robin'
    ),
    genre := (select Genre filter .name = 'Drama' limit 1),
};

insert Movie {
    title := 'Saving Private Ryan',
    release_year := 1998,
    actors := (
        select Person { @role := 'Captain Miller' } filter .first_name = 'Tom'
    ),
    director := (
        select Person { @bar := 'bar' } filter .last_name = 'Spielberg' limit 1
    ),
    genre := (select Genre filter .name = 'Drama' limit 1),
};

insert novel {
    title := 'Hunger Games',
    pages := 374,
    genre := (select Genre filter .name = 'Fiction' limit 1),
    chapters := {
        'Part 1',
        'Part 2',
        'Part 3',
    },
};

insert Book {
    title:='Chronicles of Narnia',
    pages := 206,
    chapters := {
        'Lucy looks into a wardrobe',
        'What Lucy found there',
        'Edmund and the wardrobe',
        'Turkish delight',
    },
    genre:= (select Genre filter .name = 'Fiction' limit 1)
};

insert Content {
    title := 'Halo 3',
    genre := (select Genre filter .name = 'Fiction' limit 1)
};

set global filter_title := 'summary';
insert ContentSummary;
reset global filter_title;

insert default::links::C {
    a := {(insert default::links::A), (insert default::links::A)},
    prop := (insert default::links::A),
    vals := {"1", "2", "3", "4"},
};
