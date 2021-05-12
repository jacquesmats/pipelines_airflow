class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    create_all_table = """
        CREATE TABLE staging_events(
        event_id       int            identity(0,1)    not null,
        artist         varchar,
        auth           varchar,
        firstname     varchar,
        gender         varchar,
        item_session   integer,
        lastname      varchar,
        length         float,
        level          varchar,
        location       varchar,
        method         varchar,
        page           varchar,
        registration   varchar,
        sessionid     integer,
        song           varchar DISTKEY,
        status         integer,
        ts             bigint SORTKEY,
        useragent      varchar,
        userid        integer 
    );

    CREATE TABLE staging_songs(
        num_songs           integer,
        artist_id           varchar,
        artist_latitude     float,
        artist_longitude    float,
        artist_location     varchar,
        artist_name         varchar,
        song_id             varchar,
        title               varchar DISTKEY,
        duration            float,
        year                integer                
    );

    CREATE TABLE songplays(
        songplay_id    varchar         not null,
        start_time     timestamp       not null SORTKEY,
        user_id        integer         not null,
        level          varchar      not null,
        song_id        varchar         ,
        artist_id      varchar         ,
        session_id     integer        ,
        location       varchar,
        user_agent     varchar            
    );

    CREATE TABLE users(
        user_id        integer         not null SORTKEY,
        first_name     varchar     not null,
        last_name      varchar     not null,
        gender         varchar      not null,
        level          varchar      not null    
    );

    CREATE TABLE songs(
        song_id        varchar         not null DISTKEY,
        title          varchar         not null,
        artist_id      varchar         not null,
        year           integer         not null SORTKEY,
        duration       float           not null    
    );

    CREATE TABLE artists(
        artist_id      varchar         not null SORTKEY,
        name           varchar,
        location       varchar,
        lattitude      float,
        longitude      float    
    );


    CREATE TABLE time(
        start_time     timestamp       not null SORTKEY,
        hour           integer         not null,
        day            integer         not null,
        week           integer         not null,
        month          integer         not null,
        year           integer         not null,
        weekday        integer         not null       
    );
    """