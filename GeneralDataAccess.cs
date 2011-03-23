using System;
using System.Data;
using System.Data.SqlClient;
using PlanetaryDBLib.Logger;

namespace PlanetaryDBLib.DataAccess
{
    public class SQLOps
    {
        //FJC = First Join Column
        //SJC = Second Join Column

        public static DataTable Join(DataTable first, DataTable second, DataColumn[] fjc, DataColumn[] sjc)
        {
            //Create Empty Table
            var table = new DataTable("Join");
            // Use a DataSet to leverage DataRelation
            using (var ds = new DataSet())
            {
                //Add Copy of Tables
                ds.Tables.AddRange(new[] {first.Copy(), second.Copy()});
                //Identify Joining Columns from first
                var parentcolumns = new DataColumn[fjc.Length];
                for (int i = 0; i < parentcolumns.Length; i++)
                {
                    parentcolumns[i] = ds.Tables[0].Columns[fjc[i].ColumnName];
                }

                //Identify Joining Columns from second
                var childcolumns = new DataColumn[sjc.Length];
                for (int i = 0; i < childcolumns.Length; i++)
                {
                    childcolumns[i] = ds.Tables[1].Columns[sjc[i].ColumnName];
                }

                //Create DataRelation
                var r = new DataRelation(string.Empty, parentcolumns, childcolumns, false);
                ds.Relations.Add(r);
                //Create Columns for JOIN table

                for (int i = 0; i < first.Columns.Count; i++)
                {
                    table.Columns.Add(first.Columns[i].ColumnName, first.Columns[i].DataType);
                }

                for (int i = 0; i < second.Columns.Count; i++)
                {
                    //Beware Duplicates
                    if (!table.Columns.Contains(second.Columns[i].ColumnName))
                        table.Columns.Add(second.Columns[i].ColumnName, second.Columns[i].DataType);
                    else
                        table.Columns.Add(second.Columns[i].ColumnName + "_Second", second.Columns[i].DataType);
                }

                //Loop through first table
                table.BeginLoadData();
                foreach (DataRow firstrow in ds.Tables[0].Rows)
                {
                    //Get "joined" rows
                    DataRow[] childrows = firstrow.GetChildRows(r);
                    if (childrows != null && childrows.Length > 0)
                    {
                        object[] parentarray = firstrow.ItemArray;
                        foreach (DataRow secondrow in childrows)
                        {
                            object[] secondarray = secondrow.ItemArray;
                            var joinarray = new object[parentarray.Length + secondarray.Length];
                            Array.Copy(parentarray, 0, joinarray, 0, parentarray.Length);
                            Array.Copy(secondarray, 0, joinarray, parentarray.Length, secondarray.Length);
                            table.LoadDataRow(joinarray, true);
                        }
                    }
                }
                table.EndLoadData();
            }
            return table;
        }

        public static DataTable Join(DataTable first, DataTable second, DataColumn fjc, DataColumn sjc)
        {
            return Join(first, second, new[] {fjc}, new[] {sjc});
        }

        public static DataTable Join(DataTable first, DataTable second, string fjc, string sjc)
        {
            return Join(first, second, new[] {first.Columns[fjc]}, new[] {second.Columns[sjc]});
        }
    }

    public class DataAccess
    {
        //private  
        //**************************************
        //* Purpose: Accessing SQL database 
        //*Methods:
        //*GetDataSet
        //*RunProc
        //*GetDataReader
        //*GetDataView
        //* ************************************* 
        public bool BulkCopyData(string strConnect, string bulkCopyTable, SqlDataReader sourcedata)
        {
            //********************************
            //* Purpose: Performs bulk copy from one data source to another
            //* Input parameters:
            //* bulkCopytable ---the target table to bulk data into
            //* sourcedata ---the SqlDataReader holding the data to bulk insert
            //* Returns :
            //* nothing
            //**************************************************
            bool err = false;
            //create source connection
            var sourceConnection = new SqlConnection(strConnect);
            sourceConnection.Open();
            // Create SqlBulkCopy
            var bulkData = new SqlBulkCopy(strConnect, SqlBulkCopyOptions.TableLock)
                               {
                                   BatchSize = 1000,
                                   BulkCopyTimeout = 360,
                                   DestinationTableName = bulkCopyTable
                               };
            //set number of records to process in one batch
            //set timeout for a single bulk process
            // Set destination table name

            try
            {
                bulkData.WriteToServer(sourcedata);
            }
            catch (Exception e)
            {
                err = true;
                PLOG.Write(e.Message, 1);
                bulkData.Close();
                sourceConnection.Close();
                sourceConnection.Dispose();
            }
            finally
            {
                bulkData.Close();
                sourceConnection.Close();
                sourceConnection.Dispose();
            }
            return err;
        }

        public DataSet GetDataSet(string strConnect, string[] procName, string[] dataTable)
        {
            //********************************
            //* Purpose: Returns Dataset for one or multi datatables 
            //* Input parameters:
            //*procName() ---StoredProcedures name in array
            //*dataTable()---dataTable name in array
            //* Returns :
            //*DataSet Object contains data
            //**************************************************
            SqlDataAdapter dadEorder;
            var dstEorder = new DataSet();
            var conn = new SqlConnection(strConnect);
            try
            {
                int intCnt = procName.GetUpperBound(0);
                // if one datatable and SP
                if (intCnt == 0)
                {
                    dadEorder = new SqlDataAdapter(procName[0], conn);
                    dadEorder.Fill(dstEorder, dataTable[0]);
                }
                    // more than one datatable and one SP
                else
                {
                    conn.Open();
                    //add first data table and first SP
                    dadEorder = new SqlDataAdapter(procName[0], conn);
                    dadEorder.Fill(dstEorder, dataTable[0]);
                    // add second datatable and second SP onwards
                    for (int i = 1; i < (intCnt + 1); i++)
                    {
                        dadEorder.SelectCommand = new SqlCommand(procName[i], conn);
                        dadEorder.Fill(dstEorder, dataTable[i]);
                    }
                }
                return dstEorder;
            }
            catch (Exception e)
            {
                PLOG.Write(e.Message, 1);
                throw;
            }
        }

        public void RunProc(string strConnect, string procName)
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
                PLOG.Write(e.Message, 1);
                throw;
            }
        }

        public SqlDataReader GetDataReader(string strConnect, string procName)
        {
            //**************************************
            //* Purpose: Getting DataReader for the given Procedure
            //* Input parameters:
            //*procName ---StoredProcedures name
            //* Returns :
            //*DataReader contains data
            //* ************************************
            string strCommandText = procName;
            SqlDataReader objDataReader;
            //create a new Connection object using the connection string
            var objConnect = new SqlConnection(strConnect);
            //create a new Command using the CommandText and Connection object
            var objCommand = new SqlCommand(strCommandText, objConnect) {CommandTimeout = 3600};
            //set the timeout in seconds

            try
            {
                //open the connection and execute the command
                objConnect.Open();
                //objDataAdapter.SelectCommand = objCommand
                objDataReader = objCommand.ExecuteReader(CommandBehavior.CloseConnection);
            }

            catch (Exception e)
            {
                PLOG.Write(e.Message, 1);
                throw;
            }
            return objDataReader;
        }

        public DataView GetDataView(string strConnect, string procName, string dataSetTable)
        {
            //*****************************************
            //* Purpose: Getting DataReader for the given Procedure
            //* Input parameters:
            //* procName ---StoredProcedures name
            //* dataSetTable--dataSetTable name sting
            //* Returns :
            //* DataView contains data
            //* ****************************************
            string strCommandText = procName;
            DataView objDataView;
            //create a new Connection object using the connection string
            var objConnect = new SqlConnection(strConnect);
            //create a new Command using the CommandText and Connection object
            var objCommand = new SqlCommand(strCommandText, objConnect);
            //declare a variable to hold a DataAdaptor object
            var objDataAdapter = new SqlDataAdapter();
            var objDataSet = new DataSet(dataSetTable);

            //set the timeout in seconds
            objCommand.CommandTimeout = 3600;

            try
            {
                //open the connection and execute the command
                objConnect.Open();
                objDataAdapter.SelectCommand = objCommand;
                objDataAdapter.Fill(objDataSet);

                objDataView = new DataView(objDataSet.Tables[0]);

                //objDataReader = objCommand.ExecuteReader()
            }
            catch (Exception e)
            {
                PLOG.Write(e.Message, 1);
                throw;
            }
            return objDataView;
        }

        public DataTable GetDataTable(string strConnect, string procName)
        {
            //********************************
            //* Purpose: Returns DataTable for one datatables 
            //* Input parameters:
            //*procName ---StoredProcedure or query name
            //* Returns :
            //*DataTable Object contains data
            //**************************************************
            using (var dstEorder = new DataTable())
            {
                using (var conn = new SqlConnection(strConnect))
                {
                    PLOG.Write(procName);
                    using (var dadEorder = new SqlDataAdapter(procName, conn))
                    {
                        dadEorder.Fill(dstEorder);
                    }
                    // more than one datatable and one SP
                }
                return dstEorder;
            }
        }
    }
}