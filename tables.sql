CREATE TABLE jokes (
    id INTEGER PRIMARY KEY,
    type TEXT,
    setup TEXT,
    punchline TEXT
);

select * from jokes;

CREATE TABLE random_users (
    uuid TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    created TIMESTAMP DEFAULT now()
);

select * from random_users;
