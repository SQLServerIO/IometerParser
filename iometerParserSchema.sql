DROP TABLE [dbo].[AccessSpecificationDetail]
GO
CREATE TABLE [dbo].[AccessSpecificationDetail] (
  [AccessSpecificationName] [VARCHAR](50) NOT  NULL,
  [Size]                    [INT]   NULL,
  [PercentOfSize]           [INT]   NULL,
  [Reads]                   [INT]   NULL,
  [Random]                  [INT]   NULL,
  [Delay]                   [INT]   NULL,
  [Burst]                   [INT]   NULL,
  [Align]                   [INT]   NULL,
  [Reply]                   [INT]   NULL)
GO
DROP TABLE [dbo].[AccessSpecificationHeader]
GO
CREATE TABLE [dbo].[AccessSpecificationHeader] (
  [AccessSpecificationName] [VARCHAR](50) NOT  NULL,
  [DefaultAssignment]       [INT]   NULL)
GO
ALTER TABLE [dbo].[AccessSpecificationHeader] ADD PRIMARY KEY (AccessSpecificationName)
GO
DROP TABLE [dbo].[TestHeader]
GO
CREATE TABLE [dbo].[TestHeader] (
  [TestID]          UNIQUEIDENTIFIER   NOT NULL,
  [TestType]        [INT]   NULL,
  [TestDescription] [VARCHAR](50)   NULL,
  [Version]         [VARCHAR](10)   NULL,
  [TimeStamp]       [DATETIME]   NULL)
DROP TABLE TestDetails
GO
CREATE TABLE TestDetails (
  [TestID]                     UNIQUEIDENTIFIER   NOT NULL,
  TargetType                   VARCHAR(50)   NULL,
  TargetName                   VARCHAR(50)   NULL,
  AccessSpecificationName      VARCHAR(50)   NULL,
  #Managers                    INT   NULL,
  #Workers                     INT   NULL,
  #Disks                       INT   NULL,
  IOps                         DECIMAL(38,6)   NULL,
  ReadIOps                     DECIMAL(38,6)   NULL,
  WriteIOps                    DECIMAL(38,6)   NULL,
  MBps                         DECIMAL(38,6)   NULL,
  ReadMBps                     DECIMAL(38,6)   NULL,
  WriteMBps                    DECIMAL(38,6)   NULL,
  TransactionsperSecond        DECIMAL(38,6)   NULL,
  ConnectionsperSecond         DECIMAL(38,6)   NULL,
  AverageResponseTime          DECIMAL(38,6)   NULL,
  AverageReadResponseTime      DECIMAL(38,6)   NULL,
  AverageWriteResponseTime     DECIMAL(38,6)   NULL,
  AverageTransactionTime       DECIMAL(38,6)   NULL,
  AverageConnectionTime        DECIMAL(38,6)   NULL,
  MaximumResponseTime          DECIMAL(38,6)   NULL,
  MaximumReadResponseTime      DECIMAL(38,6)   NULL,
  MaximumWriteResponseTime     DECIMAL(38,6)   NULL,
  MaximumTransactionTime       DECIMAL(38,6)   NULL,
  MaximumConnectionTime        DECIMAL(38,6)   NULL,
  Errors                       INT   NULL,
  ReadErrors                   INT   NULL,
  WriteErrors                  INT   NULL,
  BytesRead                    BIGINT   NULL,
  BytesWritten                 BIGINT   NULL,
  ReadIOs                      BIGINT   NULL,
  WriteIOs                     BIGINT   NULL,
  Connections                  INT   NULL,
  TransactionsperConnection    BIGINT   NULL,
  TotalRawReadResponseTime     BIGINT   NULL,
  TotalRawWriteResponseTime    BIGINT   NULL,
  TotalRawTransactionTime      BIGINT   NULL,
  TotalRawConnectionTime       BIGINT   NULL,
  MaximumRawReadResponseTime   BIGINT   NULL,
  MaximumRawWriteResponseTime  BIGINT   NULL,
  MaximumRawTransactionTime    BIGINT   NULL,
  MaximumRawConnectionTime     BIGINT   NULL,
  TotalRawRunTime              BIGINT   NULL,
  StartingSector               BIGINT   NULL,
  MaximumSize                  BIGINT   NULL,
  QueueDepth                   INT   NULL,
  PercentCPUUtilization        DECIMAL(38,6)   NULL,
  PercentUserTime              DECIMAL(38,6)   NULL,
  PercentPrivilegedTime        DECIMAL(38,6)   NULL,
  PercentDPCTime               DECIMAL(38,6)   NULL,
  PercentInterruptTime         DECIMAL(38,6)   NULL,
  ProcessorSpeed               DECIMAL(38,6)   NULL,
  InterruptsperSecond          DECIMAL(38,6)   NULL,
  CPUEffectiveness             DECIMAL(38,6)   NULL,
  Packets_Second               DECIMAL(38,6)   NULL,
  PacketErrors                 BIGINT   NULL,
  SegmentsRetransmitted_Second DECIMAL(38,6)   NULL,
  DateStamp                    DATETIME   NULL)

USE [IOMeterResults];
GO
IF OBJECT_ID('[dbo].[usp_AccessSpecificationHeaderInsert]') IS NOT NULL
  BEGIN
    DROP PROC [dbo].[usp_AccessSpecificationHeaderInsert]
  END
GO
CREATE PROC [dbo].[usp_AccessSpecificationHeaderInsert]
           @AccessSpecificationName VARCHAR(50),
           @DefaultAssignment       INT
AS
  SET NOCOUNT  ON
  SET XACT_ABORT  ON
  IF NOT EXISTS (SELECT 
                  1
                 FROM   
                  [dbo].[AccessSpecificationHeader]
                 WHERE  [AccessSpecificationName] = @AccessSpecificationName)
    BEGIN
      BEGIN TRAN
      INSERT INTO [dbo].[AccessSpecificationHeader]
                 ([AccessSpecificationName],
                  [DefaultAssignment])
      SELECT 
        @AccessSpecificationName,
        @DefaultAssignment
      COMMIT
    END
GO

IF OBJECT_ID('[dbo].[usp_AccessSpecificationDetailInsert]') IS NOT NULL
  BEGIN
    DROP PROC [dbo].[usp_AccessSpecificationDetailInsert]
  END
GO
CREATE PROC [dbo].[usp_AccessSpecificationDetailInsert]
           @AccessSpecificationName VARCHAR(50),
           @Size                    INT,
           @PercentOfSize           INT,
           @Reads                   INT,
           @Random                  INT,
           @Delay                   INT,
           @Burst                   INT,
           @Align                   INT,
           @Reply                   INT
AS
  SET NOCOUNT  ON
  SET XACT_ABORT  ON
  IF NOT EXISTS (SELECT 
                  1
                 FROM   
                  [dbo].[AccessSpecificationDetail]
                 WHERE  [AccessSpecificationName] = @AccessSpecificationName
                 AND Size = @Size
                 AND PercentOfSize = @PercentOfSize
                 AND Reads = @Reads
                 AND Random = @Random
                 AND [Delay] = @Delay
                 AND Burst = @Burst
                 AND Align = @Align
                 AND Reply = @Reply)
    BEGIN
      BEGIN TRAN
      INSERT INTO [dbo].[AccessSpecificationDetail]
                 ([AccessSpecificationName],
                  [Size],
                  [PercentOfSize],
                  [Reads],
                  [Random],
                  [Delay],
                  [Burst],
                  [Align],
                  [Reply])
      SELECT 
        @AccessSpecificationName,
        @Size,
        @PercentOfSize,
        @Reads,
        @Random,
        @Delay,
        @Burst,
        @Align,
        @Reply
      COMMIT
    END
GO
