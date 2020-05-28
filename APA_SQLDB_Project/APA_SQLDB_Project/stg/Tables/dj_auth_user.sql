CREATE TABLE [stg].[dj_auth_user] (
    [id]          INT           NOT NULL,
    [username]    VARCHAR (150) NULL,
    [first_name]  VARCHAR (30)  NULL,
    [last_name]   VARCHAR (30)  NULL,
    [date_joined] DATETIME      NULL,
    CONSTRAINT [auth_user_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

