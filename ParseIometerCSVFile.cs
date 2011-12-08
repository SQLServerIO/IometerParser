using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Reflection;
using NDesk.Options;

namespace IometerParser
{
    internal class ParseIometerCSVFile
    {
        private static void Main(string[] args)
        {
            Version v = Assembly.GetExecutingAssembly().GetName().Version;
            Console.WriteLine("IometerParser.exe " + v.Major + "." + v.Minor + "." + v.Build + "." + v.Minor);
            int parseerr = ParseCommandLine(args);
            if (parseerr == 1)
                return;

            var importFileList = new List<string>();

            #region constructors
            //the data holder
            var ioMeterDataSet = new DataSet();
            DataTable ioMeterTestHeader = ioMeterDataSet.Tables.Add();
            DataTable ioMeterTestRecordDetail = ioMeterDataSet.Tables.Add();
            DataTable ioMeterAccessSpecificationHeader = ioMeterDataSet.Tables.Add();
            DataTable ioMeterAccessSpecificationDetail = ioMeterDataSet.Tables.Add();

            //raw data
            ioMeterTestHeader.Columns.Add("TestID", typeof (SqlGuid));
            ioMeterTestHeader.Columns.Add("TestType", typeof (Int32));
            ioMeterTestHeader.Columns.Add("TestDescription", typeof (string));
            ioMeterTestHeader.Columns.Add("Version", typeof (string));
            ioMeterTestHeader.Columns.Add("TimeStamp", typeof (DateTime));

            ioMeterAccessSpecificationHeader.Columns.Add("AccessSpecificationName", typeof (string));
            ioMeterAccessSpecificationHeader.Columns.Add("DefaultAssignment", typeof (Int32));

            ioMeterAccessSpecificationDetail.Columns.Add("AccessSpecificationName", typeof (string));
            ioMeterAccessSpecificationDetail.Columns.Add("Size", typeof (Int32));
            ioMeterAccessSpecificationDetail.Columns.Add("PercentOfSize", typeof (Int32));
            ioMeterAccessSpecificationDetail.Columns.Add("Reads", typeof (Int32));
            ioMeterAccessSpecificationDetail.Columns.Add("Random", typeof (Int32));
            ioMeterAccessSpecificationDetail.Columns.Add("Delay", typeof (Int32));
            ioMeterAccessSpecificationDetail.Columns.Add("Burst", typeof (Int32));
            ioMeterAccessSpecificationDetail.Columns.Add("Align", typeof (Int32));
            ioMeterAccessSpecificationDetail.Columns.Add("Reply", typeof (Int32));

            ioMeterTestRecordDetail.Columns.Add("TestID", typeof (SqlGuid));
            ioMeterTestRecordDetail.Columns.Add("TargetType", typeof (string));
            ioMeterTestRecordDetail.Columns.Add("TargetName", typeof (string));
            ioMeterTestRecordDetail.Columns.Add("AccessSpecificationName", typeof (string));
            ioMeterTestRecordDetail.Columns.Add("#Managers", typeof (Int32));
            ioMeterTestRecordDetail.Columns.Add("#Workers", typeof (Int32));
            ioMeterTestRecordDetail.Columns.Add("#Disks", typeof (Int32));
            ioMeterTestRecordDetail.Columns.Add("IOps", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("ReadIOps", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("WriteIOps", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("MBps", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("ReadMBps", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("WriteMBps", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("TransactionsperSecond", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("ConnectionsperSecond", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("AverageResponseTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("AverageReadResponseTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("AverageWriteResponseTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("AverageTransactionTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("AverageConnectionTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("MaximumResponseTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("MaximumReadResponseTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("MaximumWriteResponseTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("MaximumTransactionTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("MaximumConnectionTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("Errors", typeof (Int32));
            ioMeterTestRecordDetail.Columns.Add("ReadErrors", typeof (Int32));
            ioMeterTestRecordDetail.Columns.Add("WriteErrors", typeof (Int32));
            ioMeterTestRecordDetail.Columns.Add("BytesRead", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("BytesWritten", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("ReadIOs", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("WriteIOs", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("Connections", typeof (Int32));
            ioMeterTestRecordDetail.Columns.Add("TransactionsperConnection", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("TotalRawReadResponseTime", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("TotalRawWriteResponseTime", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("TotalRawTransactionTime", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("TotalRawConnectionTime", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("MaximumRawReadResponseTime", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("MaximumRawWriteResponseTime", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("MaximumRawTransactionTime", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("MaximumRawConnectionTime", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("TotalRawRunTime", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("StartingSector", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("MaximumSize", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("QueueDepth", typeof (Int32));
            ioMeterTestRecordDetail.Columns.Add("PercentCPUUtilization", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("PercentUserTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("PercentPrivilegedTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("PercentDPCTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("PercentInterruptTime", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("ProcessorSpeed", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("InterruptsperSecond", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("CPUEffectiveness", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("Packets_Second ", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("PacketErrors", typeof (long));
            ioMeterTestRecordDetail.Columns.Add("SegmentsRetransmitted_Second", typeof (decimal));
            ioMeterTestRecordDetail.Columns.Add("DateStamp", typeof (DateTime));
            #endregion

            if (GlobalVariables.IOMeterCSVDirectoryName != null)
            {
                GlobalVariables.IOMeterCSVDirectoryName =
                    GlobalVariables.IOMeterCSVDirectoryName.Replace('"', ' ').Trim();
                Console.WriteLine("Directory To Process:" + GlobalVariables.IOMeterCSVDirectoryName);
                try
                {
                    importFileList.AddRange(
                        Directory.GetFiles(GlobalVariables.IOMeterCSVDirectoryName).Where(
                            fileName => !FileInUse(fileName)));
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to import files");
                    Console.WriteLine(e.Message);
                }
            }
            else
            {
                importFileList.Add(GlobalVariables.IOMeterCSVFileName);
            }

            StreamReader re;

            foreach (string fileName in importFileList)
            {
                try
                {
                    re = File.OpenText(fileName);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    return;
                }

                try
                {
                    GlobalVariables.TestDate = File.GetCreationTime(fileName);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    return;
                }
                Console.WriteLine("Processing File:" + fileName + "...");
                Console.WriteLine();

                //common search strings used to mark record boundaries
                const string idxRowsSearch =
                    "'Target Type,Target Name,Access Specification Name,# Managers,# Workers,# Disks,IOps,Read IOps,Write IOps,MBps,Read MBps,Write MBps,Transactions per Second,Connections per Second,Average Response Time,Average Read Response Time,Average Write Response Time,Average Transaction Time,Average Connection Time,Maximum Response Time,Maximum Read Response Time,Maximum Write Response Time,Maximum Transaction Time,Maximum Connection Time,Errors,Read Errors,Write Errors,Bytes Read,Bytes Written,Read I/Os,Write I/Os,Connections,Transactions per Connection,Total Raw Read Response Time,Total Raw Write Response Time,Total Raw Transaction Time,Total Raw Connection Time,Maximum Raw Read Response Time,Maximum Raw Write Response Time,Maximum Raw Transaction Time,Maximum Raw Connection Time,Total Raw Run Time,Starting Sector,Maximum Size,Queue Depth,% CPU Utilization,% User Time,% Privileged Time,% DPC Time,% Interrupt Time,Processor Speed,Interrupts per Second,CPU Effectiveness,Packets/Second,Packet Errors,Segments Retransmitted/Second";
                const string idxTestTypeSearch = "'Test Type,Test Description";
                const string idxTimeStampSearch = "'Time Stamp";
                const string idxAccessspecificationsSearch = "'Access specification name,default assignment";
                const string idxAccessspecificationsSearchEnd = "'End access specifications";

                var fileinput = new List<string>();

                while (re.EndOfStream == false)
                {
                    fileinput.Add(re.ReadLine());
                }
                //test header
                int idxVersion = fileinput.IndexOf("'Version");
                TestHeader.Version = fileinput[idxVersion + 1];

                int idxTestType = fileinput.IndexOf(idxTestTypeSearch);
                string[] splitInput = fileinput[idxTestType + 1].Split(',');
                TestHeader.TestType = Convert.ToInt32(splitInput[0]);
                TestHeader.TestDescription = splitInput[1];
                int idxTimeStamp = fileinput.IndexOf(idxTimeStampSearch);
                TestHeader.TimeStamp = Convert.ToDateTime(fileinput[idxTimeStamp + 1].Substring(0, 19));
                TestHeader.TestId = Guid.NewGuid();
                ioMeterTestHeader.Rows.Add(TestHeader.TestId, TestHeader.TestType, TestHeader.TestDescription,
                                           TestHeader.Version, TestHeader.TimeStamp);

                //Access Specifications Header
                int idxAccessspecifications = fileinput.IndexOf(idxAccessspecificationsSearch);
                int idxAccessspecificationsend = fileinput.IndexOf(idxAccessspecificationsSearchEnd);

                splitInput = fileinput[idxAccessspecifications + 1].Split(',');
                AccessSpecificationHeader.AccessSpecificationName = splitInput[0];
                string accessheadernameholder = AccessSpecificationHeader.AccessSpecificationName;

                ioMeterAccessSpecificationHeader.Rows.Add(splitInput[0], Convert.ToInt32(splitInput[1]));

                //Access Specifications Detail
                int i = idxAccessspecifications + 3;
                while (i < idxAccessspecificationsend)
                {
                    string[] splitInput2 = fileinput[i].Split(',');
                    ioMeterAccessSpecificationDetail.Rows.Add(AccessSpecificationHeader.AccessSpecificationName,
                                                              Convert.ToInt32(splitInput2[0]),
                                                              Convert.ToInt32(splitInput2[1]),
                                                              Convert.ToInt32(splitInput2[2]),
                                                              Convert.ToInt32(splitInput2[3]),
                                                              Convert.ToInt32(splitInput2[4]),
                                                              Convert.ToInt32(splitInput2[5]),
                                                              Convert.ToInt32(splitInput2[6]),
                                                              Convert.ToInt32(splitInput2[7]));
                    i++;
                }

                int l = 0;
                int idxResults = fileinput.IndexOf("'Results");
                int idxRows = fileinput.IndexOf(idxRowsSearch);

                idxTimeStamp = fileinput.IndexOf(idxTimeStampSearch, idxTimeStamp + 1);
                while (l <= fileinput.Count)
                {
                    i = idxRows + 1;
                    while (i < idxTimeStamp)
                    {
                        // Console.WriteLine(fileinput[i]);
                        string[] splitInput3 = fileinput[i].Replace(",,", ",0,").Replace(",,", ",0,").Split(',');
                        decimal
                            segmentsRetransmittedSecond = 0;

                        if (splitInput3[55] != "")
                            segmentsRetransmittedSecond = Convert.ToDecimal(splitInput3[55]);

                        ioMeterTestRecordDetail.Rows.Add(
                            TestHeader.TestId,
                            splitInput3[0],
                            splitInput3[1],
                            AccessSpecificationHeader.AccessSpecificationName,
                            Convert.ToInt32(splitInput3[3]),
                            Convert.ToInt32(splitInput3[4]),
                            Convert.ToInt32(splitInput3[5]),
                            Convert.ToDecimal(splitInput3[6]),
                            Convert.ToDecimal(splitInput3[7]),
                            Convert.ToDecimal(splitInput3[8]),
                            Convert.ToDecimal(splitInput3[9]),
                            Convert.ToDecimal(splitInput3[10]),
                            Convert.ToDecimal(splitInput3[11]),
                            Convert.ToDecimal(splitInput3[12]),
                            Convert.ToDecimal(splitInput3[13]),
                            Convert.ToDecimal(splitInput3[14]),
                            Convert.ToDecimal(splitInput3[15]),
                            Convert.ToDecimal(splitInput3[16]),
                            Convert.ToDecimal(splitInput3[17]),
                            Convert.ToDecimal(splitInput3[18]),
                            Convert.ToDecimal(splitInput3[19]),
                            Convert.ToDecimal(splitInput3[20]),
                            Convert.ToDecimal(splitInput3[21]),
                            Convert.ToDecimal(splitInput3[22]),
                            Convert.ToDecimal(splitInput3[23]),
                            Convert.ToInt32(splitInput3[24]),
                            Convert.ToInt32(splitInput3[25]),
                            Convert.ToInt32(splitInput3[26]),
                            Convert.ToInt64(splitInput3[27]),
                            Convert.ToInt64(splitInput3[28]),
                            Convert.ToInt64(splitInput3[29]),
                            Convert.ToInt64(splitInput3[30]),
                            Convert.ToInt32(splitInput3[31]),
                            Convert.ToInt32(splitInput3[32]),
                            Convert.ToInt64(splitInput3[33]),
                            Convert.ToInt64(splitInput3[34]),
                            Convert.ToInt64(splitInput3[35]),
                            Convert.ToInt64(splitInput3[36]),
                            Convert.ToInt64(splitInput3[37]),
                            Convert.ToInt64(splitInput3[38]),
                            Convert.ToInt64(splitInput3[39]),
                            Convert.ToInt64(splitInput3[40]),
                            Convert.ToInt64(splitInput3[41]),
                            Convert.ToInt64(splitInput3[42]),
                            Convert.ToInt32(splitInput3[43]),
                            Convert.ToDecimal(splitInput3[44]),
                            Convert.ToDecimal(splitInput3[45]),
                            Convert.ToDecimal(splitInput3[46]),
                            Convert.ToDecimal(splitInput3[47]),
                            Convert.ToDecimal(splitInput3[48]),
                            Convert.ToDecimal(splitInput3[49]),
                            Convert.ToDecimal(splitInput3[50]),
                            Convert.ToDecimal(splitInput3[51]),
                            Convert.ToDecimal(splitInput3[52]),
                            Convert.ToDecimal(splitInput3[53]),
                            Convert.ToDecimal(splitInput3[54]),
                            segmentsRetransmittedSecond,
                            TestHeader.TimeStamp
                            );
                        i++;
                    }
                    //Test Header and Detail
                    if (idxVersion - 4 < idxTimeStamp)
                    {
                        idxVersion = fileinput.IndexOf("'Version", idxVersion + 1);

                        if (idxVersion != -1)
                        {
                            if (idxVersion - 4 == idxTimeStamp)
                            {
                                TestHeader.Version = fileinput[idxVersion + 1];

                                idxTestType = fileinput.IndexOf(idxTestTypeSearch, idxTestType + 1);
                                string[] splitInput4 = fileinput[idxTestType + 1].Split(',');
                                DateTime timeStampHld = Convert.ToDateTime(fileinput[idxTimeStamp + 1].Substring(0, 19));
                                TestHeader.TestType = Convert.ToInt32(splitInput4[0]);
                                TestHeader.TestDescription = splitInput4[1];
                                TestHeader.TestId = Guid.NewGuid();
                                ioMeterTestHeader.Rows.Add(TestHeader.TestId, TestHeader.TestType,
                                                           TestHeader.TestDescription,
                                                           TestHeader.Version, timeStampHld);
                                idxTimeStamp = fileinput.IndexOf(idxTimeStampSearch, idxTimeStamp + 1);
                            }
                        }
                    }

                    //Access Specification Header and Detail
                    if (idxAccessspecifications - 3 < idxTimeStamp)
                    {
                        idxAccessspecifications = fileinput.IndexOf(idxAccessspecificationsSearch,
                                                                    idxAccessspecifications + 1);
                        idxAccessspecificationsend = fileinput.IndexOf(idxAccessspecificationsSearchEnd,
                                                                       idxAccessspecificationsend + 1);
                    }

                    if (idxAccessspecifications != -1)
                    {
                        if (idxAccessspecifications - 3 == idxTimeStamp)
                        {
                            splitInput = fileinput[idxAccessspecifications + 1].Split(',');

                            AccessSpecificationHeader.AccessSpecificationName = splitInput[0];

                            if (accessheadernameholder != AccessSpecificationHeader.AccessSpecificationName)
                            {
                                ioMeterAccessSpecificationHeader.Rows.Add(splitInput[0], Convert.ToInt32(splitInput[1]));

                                AccessSpecificationHeader.AccessSpecificationName = splitInput[0];

                                //Access Specifications Detail
                                int t = idxAccessspecifications + 3;
                                while (t < idxAccessspecificationsend)
                                {
                                    string[] splitInput2 = fileinput[t].Split(',');
                                    ioMeterAccessSpecificationDetail.Rows.Add(
                                        AccessSpecificationHeader.AccessSpecificationName,
                                        Convert.ToInt32(splitInput2[0]),
                                        Convert.ToInt32(splitInput2[1]),
                                        Convert.ToInt32(splitInput2[2]),
                                        Convert.ToInt32(splitInput2[3]),
                                        Convert.ToInt32(splitInput2[4]),
                                        Convert.ToInt32(splitInput2[5]),
                                        Convert.ToInt32(splitInput2[6]),
                                        Convert.ToInt32(splitInput2[7]));
                                    t++;
                                }
                                accessheadernameholder = AccessSpecificationHeader.AccessSpecificationName;
                            }
                        }
                    }

                    idxResults = fileinput.IndexOf("'Results", idxResults + 1);
                    idxRows = fileinput.IndexOf(idxRowsSearch, idxRows + 1);

                    if (idxResults != -1)
                    {
                        idxTimeStamp = fileinput.IndexOf(idxTimeStampSearch, idxResults + 1);
                        TestHeader.TimeStamp = Convert.ToDateTime(fileinput[idxTimeStamp + 1].Substring(0, 19));

                        l = idxTimeStamp + 2;
                    }
                    else
                    {
                        l = fileinput.Count + 1;
                    }
                }


                //Console.WriteLine(sqlioData.Rows.Count);
                //CSV export
                if (GlobalVariables.CSVOutFile != null)
                {
                    Console.WriteLine("Exporting CSV to: " + GlobalVariables.CSVOutFile);
                    Console.WriteLine();
                    try
                    {
                        if (File.Exists(GlobalVariables.CSVOutFile))
                            File.Delete(GlobalVariables.CSVOutFile);

                        //CreateWorkbook(ioMeterDataSet, GlobalVariables.CSVOutFile);
                        CreateCSVFile(ioMeterTestHeader, GlobalVariables.CSVOutFile);
                        CreateCSVFile(ioMeterAccessSpecificationHeader, GlobalVariables.CSVOutFile);
                        CreateCSVFile(ioMeterAccessSpecificationDetail, GlobalVariables.CSVOutFile);
                        CreateCSVFile(ioMeterTestRecordDetail, GlobalVariables.CSVOutFile);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }

                    //CreateWorkbook(ioMeterDataSet, GlobalVariables.CSVOutFile);
                }


                //sql bulk insert
                if (GlobalVariables.SQLServer != null && GlobalVariables.Database != null)
                {
                    //insert into AccessSpecificationHeader
                    foreach (DataRow row in ioMeterAccessSpecificationHeader.Rows) // Loop over the rows.
                    {
                        string conn = "Server=" + GlobalVariables.SQLServer + ";Database=" + GlobalVariables.Database +
                                      ";Trusted_Connection=True;";
                        string sql = "usp_AccessSpecificationHeaderInsert '" + row[0] + "'," + row[1];
                        RunProc(conn, sql);
                    }

                    //insert inot AccessSpecificationDetail
                    foreach (DataRow row in ioMeterAccessSpecificationDetail.Rows) // Loop over the rows.
                    {
                        string conn = "Server=" + GlobalVariables.SQLServer + ";Database=" + GlobalVariables.Database +
                                      ";Trusted_Connection=True;";
                        string sql = "usp_AccessSpecificationDetailInsert '" + row[0] + "'," + row[1] + "," + row[2] +
                                     "," +
                                     row[3] + "," + row[4] + "," + row[5] + "," + row[6] + "," + row[7] + "," + row[8];
                        RunProc(conn, sql);
                    }

                    GlobalVariables.TableName = "TestHeader";

                    Console.WriteLine("Inserting into server " + GlobalVariables.SQLServer + " database " +
                                      GlobalVariables.Database + " table " + GlobalVariables.TableName +
                                      " using a trusted connection");
                    Console.WriteLine();
                    if (!GlobalVariables.TableName.Contains("."))
                    {
                        GlobalVariables.TableName = "dbo." + GlobalVariables.TableName;
                    }
                    var bulkCopy =
                        new SqlBulkCopy(
                            "Server=" + GlobalVariables.SQLServer + ";Database=" + GlobalVariables.Database +
                            ";Trusted_Connection=True;",
                            SqlBulkCopyOptions.TableLock)
                            {
                                BatchSize = 10000,
                                DestinationTableName = GlobalVariables.TableName
                            };
                    try
                    {
                        bulkCopy.WriteToServer(ioMeterTestHeader); // SQLIOData is a DataTable type
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(GlobalVariables.TableName + " Failed to Import");
                        Console.WriteLine(e.Message);
                    }
                    finally
                    {
                        bulkCopy.Close();
                    }

                    GlobalVariables.TableName = "TestDetails";

                    Console.WriteLine("Inserting into server " + GlobalVariables.SQLServer + " database " +
                                      GlobalVariables.Database + " table " + GlobalVariables.TableName +
                                      " using a trusted connection");
                    Console.WriteLine();
                    if (!GlobalVariables.TableName.Contains("."))
                    {
                        GlobalVariables.TableName = "dbo." + GlobalVariables.TableName;
                    }
                    bulkCopy =
                        new SqlBulkCopy(
                            "Server=" + GlobalVariables.SQLServer + ";Database=" + GlobalVariables.Database +
                            ";Trusted_Connection=True;",
                            SqlBulkCopyOptions.TableLock)
                            {
                                BatchSize = 10000,
                                DestinationTableName = GlobalVariables.TableName
                            };
                    try
                    {
                        bulkCopy.WriteToServer(ioMeterTestRecordDetail); // SQLIOData is a DataTable type
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(GlobalVariables.TableName + " Failed to Import");
                        Console.WriteLine(e.Message);
                    }
                    finally
                    {
                        bulkCopy.Close();
                    }
                }
            }
            Console.WriteLine(ioMeterAccessSpecificationHeader.Rows.Count);
            Console.WriteLine(ioMeterAccessSpecificationDetail.Rows.Count);
            Console.WriteLine(ioMeterTestHeader.Rows.Count);
            Console.WriteLine(ioMeterTestRecordDetail.Rows.Count);
            Console.WriteLine("Complete");
            Console.ReadKey();
        }

        public static void RunProc(string strConnect, string procName)
        {
            //****************************************
            //* Purpose: Executing Stored Procedures where UPDATE, INSERT
            //*and DELETE statements are expected but does not 
            //*work for select statement is expected. 
            //* Input parameters: 
            //*procName ---StoredProcedures name 
            //* Returns : 
            //*nothing 
            //* ***************************************
            string strCommandText = procName;
            //create a new Connection object using the connection string
            var objConnect = new SqlConnection(strConnect);
            //create a new Command using the CommandText and Connection object
            var objCommand = new SqlCommand(strCommandText, objConnect) {CommandTimeout = 3600};
            //set the timeout in seconds

            try
            {
                objConnect.Open();
                objCommand.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                throw;
            }
        }

        public static void CreateCSVFile(DataTable dt, string strFilePath)
        {
            var sw = new StreamWriter(strFilePath, true);
            int iColCount = dt.Columns.Count;
            for (int i = 0; i < iColCount; i++)
            {
                sw.Write(dt.Columns[i]);
                if (i < iColCount - 1)
                {
                    sw.Write(",");
                }
            }

            sw.Write(sw.NewLine);
            foreach (DataRow dr in dt.Rows)
            {
                for (int i = 0; i < iColCount; i++)
                {
                    if (!Convert.IsDBNull(dr[i]))
                    {
                        sw.Write(dr[i].ToString());
                    }
                    if (i < iColCount - 1)
                    {
                        sw.Write(",");
                    }
                }
                sw.Write(sw.NewLine);
            }
            sw.WriteLine();
            sw.Close();
        }

        public static int ParseCommandLine(string[] args)
        {
            bool showHelp = false;

            var p = new OptionSet
                        {
                            {
                                "s:|sqlserver:", "The SQL Server you want to import the data into.",
                                v => GlobalVariables.SQLServer = v
                                },
                            {
                                "u:|sqluser:", "If using SQL Server authentication specify a user",
                                v => GlobalVariables.SQLServerUser = v
                                },
                            {
                                "p:|sqlpass:", "If using SQL Server authentication specify a password",
                                v => GlobalVariables.SQLServerPassword = v
                                },
                            {
                                "d:|databasename:", "The database you want to import the data into.",
                                v => GlobalVariables.Database = v
                                },
                            {
                                "f:|iometerfilename:", "The file name you want to import the data from.",
                                v => GlobalVariables.IOMeterCSVFileName = v
                                },
                            {
                                "a:|iometerfiledirectory:",
                                "The directory containing the files you want to import the data from.",
                                v => GlobalVariables.IOMeterCSVDirectoryName = v
                                },
                            {
                                "o:|csvoutputfilename:", "The file name you want to export the data to.",
                                v => GlobalVariables.CSVOutFile = v
                                },
                            {
                                "?|h|help", "show this message and exit",
                                v => showHelp = v != null
                                },
                        };
            try
            {
                p.Parse(args);
            }

            catch (OptionException e)
            {
                Console.Write("IOMeterParser Error: ");
                Console.WriteLine(e.Message);
                Console.WriteLine("Try `IOMeterParser --help' for more information.");
                return 1;
            }

            if (args.Length == 0)
            {
                ShowHelp("Error: please specify some commands....", p);
                return 1;
            }

            if (GlobalVariables.IOMeterCSVFileName == null && GlobalVariables.IOMeterCSVDirectoryName == null &&
                !showHelp)
            {
                ShowHelp("Error: You must specify a file to import (-f) or a directory to import (-a).", p);
                return 1;
            }

            if (showHelp)
            {
                ShowHelp(p);
                return 1;
            }
            return 0;
        }

        private static void ShowHelp(string message, OptionSet p)
        {
            Console.WriteLine(message);
            Console.WriteLine("Usage: IOMeterParser [OPTIONS]");
            Console.WriteLine("Process output of the IOMeter program generated CSV file.");
            Console.WriteLine();
            Console.WriteLine("Options:");
            p.WriteOptionDescriptions(Console.Out);
        }

        private static void ShowHelp(OptionSet p)
        {
            Console.WriteLine("Usage: IOMeterParser [OPTIONS]");
            Console.WriteLine("Process output of the IOMeter program generated CSV file.");
            Console.WriteLine();
            Console.WriteLine("Options:");
            p.WriteOptionDescriptions(Console.Out);
        }

        private static bool FileInUse(string path)
        {
            string message;
            try
            {
                //Just opening the file as open/create
#pragma warning disable 168
                using (var fs = new FileStream(path, FileMode.OpenOrCreate))
#pragma warning restore 168
                {
                    //If required we can check for read/write by using fs.CanRead or fs.CanWrite
                }
                return false;
            }
            catch (IOException ex)
            {
                //check if message is for a File IO
                message = ex.Message;
                if (message.Contains("The process cannot access the file"))
                    return true;
                throw;
            }
        }
    }
}