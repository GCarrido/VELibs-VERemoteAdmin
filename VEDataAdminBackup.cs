using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using System.Windows.Forms;
using Verdugo;
using WebRemote;

namespace VERemoting
{
	public partial class VEDataAdmin : VEDatabase
	{
		public string CompressMethod
		{
			set { ZipFile.CompressObject = value; }
		}

		protected string BackupTimeStamp { get { return VETimeStamp.DateStamp + " " + VETimeStamp.TimeStamp; } }

		#region Backup Server - Backs up all Server tables, procedures and functions by Database and by Schema within each Database

		public void BackupServer()
		{
			DateTime startTime = DateTime.Now;
			string[] databases = sourceCmd.ApplicationDatabases;

			Memo("Memo", string.Format("/***\r\n****\t{0}\r\n****\tBackup Server {1} - {2:n0} Databases\r\n***/\r\n",
							BackupTimeStamp, sourceObject.ServerID, // SourceServerAlias, 
							databases.Length));

			try
			{
				ZipFile.InitializeServer(databases);
				errors.Clear();

				for (int i = 0; i < databases.Length && !abort; i++)
					BackupDatabase(SourceDatabase = databases[i]);
				
				zipFile.FinalizeServer();

				foreach (Totals tTotals in zipFile.DatabaseTotals)
					Memo("Memo", string.Format("Backup Server Objects: {0, 13:n0} Bytes ({1,10:n0} Compressed) - {2,7:n0} Database {3}",
										tTotals.size, tTotals.deflated, tTotals.count, tTotals.Name));

				Totals totals = zipFile.ServerTotal;

				Memo("Memo", string.Format("{0,21}  {1, 13:n0} Bytes ({2,10:n0} Compressed) - {3,7:n0} **** Total Server [{4}] Objects\r\n{5, 54:n2}%\r\n",
									"****", totals.size, totals.deflated, totals.count, sourceObject.ServerID,
									100.0f * totals.deflated / totals.size));
			}
			catch (Exception e)
			{
				Error(new string[] { "Backup Server Error: " + SourceServerDatabase, e.Message });
			}
			finally
			{
				SourceDatabase = null;
				zipFile.Close();
				Memo("Totals", string.Format("/***\r\n****\t{0}\r\n****\tBackup Server [{1}] Databases\r\n****\t{2}\r\n***\r\n",
								BackupTimeStamp, sourceObject.ServerID,
								abort ? "*** ABORTED ***" : "Completed: " + VETimeStamp.ElapsedTime(startTime)));
				ListErrors();
				Memo("End", "");
			}
		}

		public void BackupServerTables()
		{
			BackupServerObjects("Tables");
		}

		public void BackupServerProcedures() 
		{
			BackupServerObjects("Procedures"); 
		}

		public void BackupServerFunctions() 
		{
			BackupServerObjects("Functions"); 
		}

		void BackupServerObjects(string type)
		{
			DateTime startTime = DateTime.Now;
			string[] databases = sourceCmd.ApplicationDatabases;

			Memo("Start", string.Format("/***\r\n****\t{0}\r\n****\tBackup Server [{1}] {2} - {3:n0} Databases\r\n***/\r\n",
							BackupTimeStamp, sourceObject.ServerID, type, databases.Length));

			try
			{
				ZipFile.InitializeServer(databases, type);

				foreach (string database in databases)
					if (!abort && database.ToLower().IndexOf("northwind") < 0)				//	Can't handle Northwind images
					{
						ZipFile.DeflateDatabase(database);
						BackupDatabaseObjects(SourceDatabase = database, type);
					}

				zipFile.FinalizeServer();

				foreach (Totals tTotals in zipFile.DatabaseTotals)
					Memo("Totals", string.Format("Backup Server {0}: {1, 13:n0} Bytes ({2,10:n0} Compressed) - {3,7:n0} Schema {4}",
										type, tTotals.size, tTotals.deflated, tTotals.count, tTotals.Name));

				Totals totals = zipFile.ServerTotal;
				string fmt = string.Format("{{0,{0}}} ", 14 + type.Length);

				Memo("Totals", string.Format("{0} {1, 13:n0} Bytes ({2,10:n0} Compressed) - {3,7:n0} Server [{4}] {5}\r\n",
									string.Format(fmt, "****"),
									totals.size, totals.deflated, totals.count, sourceObject.ServerID, type));
			}
			catch (Exception e)
			{
				Error(string.Format("VEDataAdmin.BackupServerObjects: [{0}] {1}", sourceObject.ServerID, type), e);
			}
			finally
			{
				SourceDatabase = null;
				zipFile.Close();
				
				string text = string.Format("/***\r\n****\t{0}\r\n****\tBackup Server [{1}] {2}\r\n****\t{3}\r\n***/\r\n",
								BackupTimeStamp, sourceObject.ServerID, type,
								abort ? "*** ABORTED ***" : "Completed: " + VETimeStamp.ElapsedTime(startTime));
				
				if (!abort)
					Memo("End", text);
				else
					Error(text);
			}
		}

		#endregion Backup Server 

		#region Backup Database - Backs up all Database tables, procedures and functions by Schema

		string textBackupDatabase = "BackupDatabase 1";

		public void BackupDatabase(bool all)
		{
			if (!all)
				BackupDatabaseStart(true);
			else
			{
				string[] databases = sourceCmd.ApplicationDatabases;

				foreach (string database in databases)
					if (!abort)
					{
						SourceDatabase = database;
						BackupDatabaseStart(false);
					}

				SourceDatabase = null;
				Memo("End", "");
			}
		}

		public void BackupDatabase() 
		{
			BackupDatabaseStart(true);
		}

		void BackupDatabaseStart(bool isEnd) 
		{
			DateTime startTime = DateTime.Now;

			bool isNorthWind = SourceDatabase.ToLower().IndexOf("northwind") >= 0;

			textBackupDatabase = "BackupDatabase 1";
			
			try
			{
				if (!isNorthWind)				//	Can't handle Northwind images
				{
					ZipFile.InitializeDatabase(SourceDatabase);
					errors.Clear();
					BackupDatabase(SourceDatabase);
				}
			}
			catch (Exception e)
			{
				Error("VEAdmin.BackupDatabase Error: " + textBackupDatabase + "\r\n" + e.Message);
			}
			finally
			{
				if (!isNorthWind) 
					zipFile.Close();

				Memo("Totals", string.Format("/***\r\n****\t{0}\r\n****\tBackup Database {1}\r\n****\t{2}\r\n***/\r\n",
								BackupTimeStamp, backupServerDatabase,
								abort ? "*** ABORTED ***" : isNorthWind ? "*** NorthWind Exclusion ***" : "Completed: " + VETimeStamp.ElapsedTime(startTime)));

				ListErrors();
				
				if (isEnd)
					Memo("End", "");
			}
		}

		void BackupDatabase(string database)
		{
			if (database.ToLower().IndexOf("northwind") < 0)
			{
				try
				{
					sourceCmd.ChangeDatabase(backupDatabase = database);

					DateTime startTime = DateTime.Now;
					string[] schemas = sourceCmd.RetrieveUsers();

					Memo("Start", string.Format("/**\r\n***\t{0}\r\n***\tBackup Database {1} - {2:n0} Schemas\r\n**/\r\n",
										BackupTimeStamp, backupServerDatabase, schemas.Length));

					ZipFile.DeflateDatabase(database);
					zipFile.InitializeSchemaNames(schemas);

					for (int i = 0; i < schemas.Length && !abort; i++)
						BackupSchema(schemas[i]);

					zipFile.FinalizeDatabase();

					foreach (Totals tTotals in zipFile.SchemaTotals)
						Memo("Totals", string.Format("Backup Database Objects: {0, 13:n0} Bytes ({1,10:n0} Compressed) - {2,7:n0} Schema {3}",
											tTotals.size, tTotals.deflated, tTotals.count, tTotals.Name));

					Totals totals = zipFile.DatabaseTotal;

					Memo("Totals", string.Format("Backup Database Objects: {0, 13:n0} Bytes ({1,10:n0} Compressed) - {2,7:n0} *** Total Database {3} Objects\r\n" +
										"Backup Database Objects: {4, 31:n1}% {5, 24} {6}\r\n",
										totals.size, totals.deflated, totals.count, database,
										totals.size > 0 ? 100.0 * totals.deflated / totals.size : 0.0,
										"***", abort ? "ABORTED" : VETimeStamp.ElapsedTime(startTime)));
				}
				catch (Exception e)
				{
					Error("VEAdmin.BackupDatabase(database): " + database, e);
				}
				finally
				{
					sourceCmd.ChangeDatabase(backupDatabase = SourceDatabase);
				}
			}
		}

		#endregion Backup Database

		#region Backup Database Objects - Backs up all Database tables, procedures or functions by Schema

		#region BackupDatabaseTables - Backs up all Database tables by Schema

		public void BackupDatabaseTables() 
		{
			try
			{
				ZipFile.InitializeDatabaseObjects(SourceDatabase, "Tables");
				BackupDatabaseObjects(SourceDatabase, "Tables");
			}
			finally
			{
				zipFile.Close();
			}
		}

		void BackupDatabaseTables(string database)
		{
			DateTime startTime = DateTime.Now;
			
			sourceCmd.ChangeDatabase(backupDatabase = database);

			string[] schemas = sourceCmd.RetrieveUsers();

			Memo("Tables", string.Format("/*\r\n**\t{0}\r\n**\tBackup Database {1} - All Tables for {2:n0} Schemas\r\n*/\r\n", 
									BackupTimeStamp, backupServerDatabase, schemas.Length));
			try
			{
				zipFile.InitializeSchemaNames(schemas);

				foreach (string schema in schemas)
				{
					BackupTables(schema);

					if (abort)
						break;
				}

				Memo("Tables", string.Format("/*\r\n**\tBackup Database {0} - All Schema Tables: {1,7} {2}\r\n*/\r\n",
					backupServerDatabase, "***", abort ? "ABORTED" : VETimeStamp.ElapsedTime(startTime)));
			}
			catch (Exception e)
			{
				Memo("Exception", string.Format("\r\nBackup Database {0} - All Schema Tables: {1,7} {2}\r\n", backupServerDatabase, "ERROR", e.Message));
			}
			finally 
			{ 
				sourceCmd.ChangeDatabase(backupDatabase = SourceDatabase); 
			}
		}

		#endregion BackupDatabaseTables - Backs up all Database tables by schema

		#region BackupDatabaseProcedures and BackupDatabaseFunctions - Backs up all Database procedures or functions by Schema

		public void BackupDatabaseProcedures() 
		{
			try
			{
				ZipFile.InitializeDatabaseObjects(SourceDatabase, "Procedures");
				BackupDatabaseObjects(SourceDatabase, "Procedures");
			}
			finally
			{
				zipFile.Close();
			}
		}

		public void BackupDatabaseFunctions() 
		{ 
			try
			{
				ZipFile.InitializeDatabaseObjects(SourceDatabase, "Functions");
				BackupDatabaseObjects(SourceDatabase, "Functions");
			}
			finally
			{
				zipFile.Close();
			}
		}

		void BackupDatabaseObjects(string database, string type)
		{
			DateTime startTime = DateTime.Now;
			
			sourceCmd.ChangeDatabase(backupDatabase = database);

			string process = "Backup Database";
			string[] schemas = sourceCmd.RetrieveUsers();

			Memo("Start", string.Format("/*\r\n**\t{0}\r\n**\t{1} {2} {3} - {4:n0} Schema{5}\r\n*/\r\n",
				BackupTimeStamp, process, backupServerDatabase, type, schemas.Length, schemas.Length > 0 ? "s" : ""));

			try
			{
				zipFile.InitializeSchemaNames(schemas);
				
				foreach (string schema in schemas)
				{
					zipFile.DeflateSchema(schema, type);

					if (type == "Tables")
						BackupTables(schema);
					else
						BackupStoredObjects(type, schema);

					zipFile.FinalizeSchema();

					if (abort)
						break;
				}

				zipFile.FinalizeDatabase();

				foreach (Totals tTotals in zipFile.SchemaTotals)
					Memo("Totals", string.Format("{0} {1}: {2, 13:n0} Bytes ({3,10:n0} Compressed) - {4,7:n0} Schema {5}",
										process, type, tTotals.size, tTotals.deflated, tTotals.count, tTotals.Name));

				Totals totals = zipFile.DatabaseTotal;

				Memo(Abort ? "Error" : "Totals", string.Format("{0} {1}: {2, 13:n0} Bytes ({3,10:n0} Compressed) - {4,7:n0} *** Total Database {5} {6}\r\n" +
									"Backup Database {0}: {6, 31:n1}% {7, 24} {8}\r\n",
									process, type,
									totals.size, totals.deflated, totals.count, database,
									totals.size > 0 ? 100.0 * totals.deflated / totals.size : 0.0,
									"***", abort ? "ABORTED" : VETimeStamp.ElapsedTime(startTime)));
			}
			catch (Exception e)
			{
				Error(string.Format("VEDataAdmin.BackupDatabaseObjects {0} - All Schema {1}", backupServerDatabase, type), e);
			}
		}

		#endregion BackupDatabaseProcedures and BackupDatabaseFunctions

		#endregion Backup Database Objects

		#region Backup Schema Objects - Backs up all Schema tables, procedures and functions

		public void BackupSchema(bool all)
		{
			if (!all)
				BackupSchemaStart(true);
			else
			{
				string[] schemas = sourceCmd.RetrieveUsers();

				foreach (string schema in schemas)
					if (!abort)
					{
						SourceSchema = schema;
						BackupSchemaStart(false);
					}

				SourceSchema = null;
				Memo("End", "");
			}
		}

		public void BackupSchema()
		{
			BackupSchemaStart(true);
		}

		void BackupSchemaStart(bool isEnd) 
		{
			try
			{
				ZipFile.InitializeSchema(backupDatabase, SourceSchema);
				BackupSchema(SourceSchema);
			}
			finally
			{
				zipFile.Close();
				ListErrors();

				if (isEnd)
					Memo("End", "");
			}
		}

		void BackupSchema(string schema)
		{
			string process = "Backup Schema Objects";

			try
			{
				DateTime startTime = DateTime.Now;
				ActiveSchema = schema;

				Memo("Start", string.Format("/*\r\n**\t{0}\r\n**\t{1}: From {2}\r\n**\t{1}:      [{3}]\r\n**\t{1}:\r\n**\t{1}:   To {4}\r\n**\t{1}:      [{5}]\r\n*/\r\n",
											BackupTimeStamp, process,
											sourceObject.DatabaseSchema, sourceObject.Server,
											zipFile.Filename, zipFile.Path));

				zipFile.DeflateSchema(schema);

				BackupStoredObjects("Functions", schema);

				if (!abort)
					BackupStoredObjects("Procedures", schema);

				if (!abort)
					BackupTables(schema);

				zipFile.FinalizeSchema();

				foreach (Totals sTotals in zipFile.SchemaTypeTotals)
					Memo("Totals", string.Format("{0}: {1, 10:n0} Bytes ({2,10:n0} Compressed) - {3,7:n0} {4}",
										process, sTotals.size, sTotals.deflated, sTotals.count, sTotals.Name));

				Totals totals = zipFile.SchemaTotal;

				Memo("Totals", string.Format("{0}: {1, 10:n0} Bytes ({2,10:n0} Compressed) - {3,7:n0} ** Total Schema {4} Objects\r\n" +
									"{0}: {5, 28:n1}% {6, 23} {7}\r\n",
									process, 
									totals.size, totals.deflated, totals.count, schema,
									totals.size > 0 ? 100.0 * totals.deflated / totals.size : 0.0,
									"**", abort ? "ABORTED" : VETimeStamp.ElapsedTime(startTime)));
			}
			catch (Exception e)
			{
				Error(string.Format("VEDataAdmin.BackupSchema: {0}", schema), e);
			}

		}

		#endregion Backup Schema Objects

		#region Backup Schema Stored Procedures and Functions

		#region BackupProcedures and BackupFunctions - Backs up all schema procedures or functions

		public void BackupFunctions() 
		{
			try
			{
				ZipFile.InitializeFunctions(sourceObject.Database, SourceSchema);
				BackupStoredObjects("Functions", SourceSchema);
			}
			finally
			{
				zipFile.Close();
				ListErrors();
				Memo("End", "");
			}
		}

		public void BackupProcedures()
		{
			try
			{
				ZipFile.InitializeProcedures(sourceObject.Database, SourceSchema);
				BackupStoredObjects("Procedures", SourceSchema);
			}
			finally
			{
				zipFile.Close();
				ListErrors();
				Memo("End", "");
			}
		}

		protected virtual Totals BackupStoredObjects(string type, string schema) { return null; }

		#endregion BackupProcedures and BackupFunctions

		#region BackupProcedure and BackupFunction - Backs up a selected procedure or function

		public virtual void BackupProcedure(string name) { }
		public virtual void BackupFunction(string name) { }

		#endregion BackupProcedure and BackupFunction

		#region BackupStoredObject - Gets the work done

		protected int BackupStoredObject(string type)
		{
			Status("Backing-up " + backupObject.SchemaObject);

			int bytes = 0;

			StreamWriter backupFile = File.CreateText(backupObject.FilePaths[type]);

			try
			{
				foreach (string text in sourceCmd.RetrieveProcedureText(backupObject.SchemaObject))
				{
					backupFile.WriteLine(text);
					bytes += text.Length + 2;

					if (Abort)
						break;
				}
			}

			finally
			{
				backupFile.Close();
				Memo("Memo", string.Format("Backup Schema {0}: {1, 7:n0} Bytes Copied To ...{2}", type, bytes, backupObject.FileName)); 
				Status("");
			}

			return bytes;
		}

		#endregion BackupStoredObject

		#endregion Backup Schema Stored Procedures and Functions

		#region BackupTables - Backs up all Schema tables

		public virtual void BackupTable(string tablename) { }

		public void BackupTables() 
		{
			try
			{
				ZipFile.InitializeTables(sourceObject.Database, SourceSchema);
				BackupTables(SourceSchema);
			}
			finally
			{
				zipFile.Close();
				ListErrors();
				Memo("End", "");
			}
		}

		protected virtual Totals BackupTables(string schema) { return null; }			//	Moved to VEAdminFiles;  counterpart in VEAdminZip

		protected DataTable backupTableDependencies;
		protected bool BackupTableDependencies()
		{
			Status("Table Dependencies");

			backupTableDependencies = GetTableDependencies(sourceCmd, backupObject.Schema);

			bool okay = backupTableDependencies != null;

			if (okay)
				zipFile.DeflateTableName(veTableDependencies);

			else
			{
				Memo("Error", string.Format("Backup Schema Tables: {0, 7} Unable to create the table dependencies file.  See error below\r\n{1}",
							"ERROR", Message));

				okay = MessageBox.Show("Table Dependencies were not created.  Do you wish to proceed?", "Backup Schema Tables",
									MessageBoxButtons.YesNo) == DialogResult.Yes;
			}

			return okay;
		}

		protected virtual void BackupTableDependencies(DataTable tableDependencies) { }

		/// <summary> Writes to 'stream'
		/// <para> 1. A string of 'tableDependencies' column names suitable for inserting records </para>
		/// <para> 2. A string of record values for every record in 'tableDependencies' suitable for inserting records </para>
		/// </summary>
		/// <param name="stream"></param>
		/// <param name="table"></param>
		protected void BackupTableDependencies(StreamWriter stream, DataTable table)
		{
			string comma = "", text = "";

			foreach (DataColumn column in table.Columns)
			{
				text += comma + column.ColumnName;
				comma = ", ";
			}

			stream.WriteLine("(" + text + ")");

			foreach (DataRow row in table.Rows)
			{
				text = comma = "";

				foreach (DataColumn column in table.Columns)
				{
					text += comma + string.Format("{0}{1}{0}", column.DataType == typeof(System.String) ? "'" : "", row[column]);
					comma = ", ";
				}

				stream.WriteLine("(" + text + ")");
			}
		}

		protected virtual int BackupTableObject(ref int recordCount)
		{
			StreamWriter backupFile = null;
			int count = 0;

			try
			{
				backupFile = File.CreateText(backupObject.FilePaths["Data"]);
				count = BackupTableObject(backupFile, ref recordCount);
			}    
			catch (Exception e)
			{
				MessageBox.Show(e.Message, "VERemoteAdmin.VEDataAdminBackup.BackupTableObject");
			}
			finally
			{
				if (backupFile!= null)
					backupFile.Close();
		    }

			return count;
		}

		/// <summary> Deflates the table specified in .backupObject.SchemaTable
		/// <para> 1. Retrieves the record count from VEDatabase.TableCount into 'recordCount' </para>
		/// <para> 2. Opens a VEDatabase DataReader to read all of the table's records </para>
		/// <para> 3. Writes to the 'backupFile' StreamWriter a string containing column names suitable for inserting records into the table </para>
		/// <para> 4. Writes to the 'backupFile' StreamWriter, for every record in the table,  a string of record values suitable for inserting records into the table</para>
		/// <para> Returns the number of record values written </para>
		/// </summary>
		/// <param name="backupFile"></param>
		/// <param name="recordCount"></param>
		/// <returns></returns>
		protected int BackupTableObject(StreamWriter backupFile, ref int recordCount)
		{
			int count = 0,
				remaining = recordCount = sourceCmd.TableCount(backupObject.SchemaTable);

			try
			{
				Abort = false;

				if (sourceCmd.OpenReader("SELECT * FROM " + backupObject.SchemaTable)) 
				{
			        backupFile.WriteLine(sourceCmd.ReaderInsertColumnNames);

			        while (!Abort && sourceCmd.ReaderRead())
			        {
			            backupFile.WriteLine(sourceCmd.ReaderInsertColumnValues);
			            count++;
			        }
				}
			}    
			catch (Exception e)
			{
				MessageBox.Show(e.Message, "VERemoteAdmin.VEDataAdminBackup.BackupTableObject2");
			}
			finally
			{
				sourceCmd.CloseReader();
		        Status("");
		    }

			return count;
		}

		#endregion Backup Schema Tables

	}
}
