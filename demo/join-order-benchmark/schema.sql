CREATE TABLE aka_name (
    id integer NOT NULL,
    person_id integer NOT NULL,
    name text NOT NULL,
    imdb_index varchar(12),
    name_pcode_cf varchar(5),
    name_pcode_nf varchar(5),
    surname_pcode varchar(5),
    md5sum varchar(32)
);

CREATE TABLE aka_title (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    title text NOT NULL,
    imdb_index varchar(12),
    kind_id integer NOT NULL,
    production_year integer,
    phonetic_code varchar(5),
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    note text,
    md5sum varchar(32)
);

CREATE TABLE cast_info (
    id integer NOT NULL,
    person_id integer NOT NULL,
    movie_id integer NOT NULL,
    person_role_id integer,
    note text,
    nr_order integer,
    role_id integer NOT NULL
);

CREATE TABLE char_name (
    id integer NOT NULL,
    name text NOT NULL,
    imdb_index varchar(12),
    imdb_id integer,
    name_pcode_nf varchar(5),
    surname_pcode varchar(5),
    md5sum varchar(32)
);

CREATE TABLE comp_cast_type (
    id integer NOT NULL,
    kind varchar(32) NOT NULL
);

CREATE TABLE company_name (
    id integer NOT NULL,
    name text NOT NULL,
    country_code varchar(255),
    imdb_id integer,
    name_pcode_nf varchar(5),
    name_pcode_sf varchar(5),
    md5sum varchar(32)
);

CREATE TABLE company_type (
    id integer NOT NULL,
    kind varchar(32) NOT NULL
);

CREATE TABLE complete_cast (
    id integer NOT NULL,
    movie_id integer,
    subject_id integer NOT NULL,
    status_id integer NOT NULL
);

CREATE TABLE info_type (
    id integer NOT NULL,
    info varchar(32) NOT NULL
);

CREATE TABLE keyword (
    id integer NOT NULL,
    keyword text NOT NULL,
    phonetic_code varchar(5)
);

CREATE TABLE kind_type (
    id integer NOT NULL,
    kind varchar(15) NOT NULL
);

CREATE TABLE link_type (
    id integer NOT NULL,
    link varchar(32) NOT NULL
);

CREATE TABLE movie_companies (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    company_id integer NOT NULL,
    company_type_id integer NOT NULL,
    note text
);

CREATE TABLE movie_info (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info text NOT NULL,
    note text
);

CREATE TABLE movie_info_idx (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info text NOT NULL,
    note text
);

CREATE TABLE movie_keyword (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    keyword_id integer NOT NULL
);

CREATE TABLE movie_link (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    linked_movie_id integer NOT NULL,
    link_type_id integer NOT NULL
);

CREATE TABLE name (
    id integer NOT NULL,
    name text NOT NULL,
    imdb_index varchar(12),
    imdb_id integer,
    gender varchar(1),
    name_pcode_cf varchar(5),
    name_pcode_nf varchar(5),
    surname_pcode varchar(5),
    md5sum varchar(32)
);

CREATE TABLE person_info (
    id integer NOT NULL,
    person_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info text NOT NULL,
    note text
);

CREATE TABLE role_type (
    id integer NOT NULL,
    role varchar(32) NOT NULL
);

CREATE TABLE title (
    id integer NOT NULL,
    title text NOT NULL,
    imdb_index varchar(12),
    kind_id integer NOT NULL,
    production_year integer,
    imdb_id integer,
    phonetic_code varchar(5),
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    series_years varchar(49),
    md5sum varchar(32)
);
