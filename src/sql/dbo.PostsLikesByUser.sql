SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PostsLikesByUser](
	[firstName] [nvarchar](max) NULL,
	[lastName] [nvarchar](max) NULL,
	[averageLikes] [float] NULL,
	[count] [bigint] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


