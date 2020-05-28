CREATE TABLE [tmp].[dj_auth_user] (
    [id]          INT           NOT NULL,
    [username]    VARCHAR (150) NOT NULL,
    [first_name]  VARCHAR (30)  NOT NULL,
    [last_name]   VARCHAR (30)  NOT NULL,
    [date_joined] DATETIME      NOT NULL,
    CONSTRAINT [auth_user_pkey] PRIMARY KEY CLUSTERED ([id] ASC)
);

