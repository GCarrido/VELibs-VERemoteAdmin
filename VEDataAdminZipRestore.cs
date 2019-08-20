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

namespace VERemoting
{
	public partial class VEAdminZip : VEDataAdmin
	{
		#region Restore

		/// <summary> Get: Returns the server of of the selected zip file  </summary>
		public override string RestoreServer { get { return ZipFile.Server; } }

		public override string RestoreDatabase { get { return ZipFile.Database; } }

		#region Restore DataTable objects

		/// <summary>Returns a table of databases found in the selected zip file</summary>
		public override DataTable RestoreDatabases { get { return ZipFile.Databases; } }

		/// <summary>Returns a table of schemas associated with the database indexed by 'dbIndex'.  See .RestoreDatabases()</summary>
		/// <param name="dbIndex"></param>
		/// <returns></returns>
		public override DataTable RestoreSchemas(int dbIndex) { return ZipFile.Schemas(dbIndex); }

		/// <summary>Returns a table of 'type' objects (i.e. Tables, Procedures, Functions) associated with the schema indexed by 'sIndex'.  See .RestoreSchemas(...)</summary>
		/// <param name="sIndex"></param>
		/// <param name="type"></param>
		/// <returns></returns>
		public override DataTable RestoreObjects(int sIndex, string type) { return ZipFile.Objects(sIndex, type); }

		#endregion DataTable objects

		#region Restore Tables

		int maxCommand = 0, minCommand = 200000, maxCommand1024 = 0, maxCommand2048 = 0;

		public override void RestoreTables(VEDelegate taskCompletion)
		{
			DateTime startTime = DateTime.Now;
			int count = 0, schemaCount = 0;
			bool failed = false;
			string process = "Restore User Tables";

			Memo("Start", string.Format("//\r\n//\t{0}\r\n//\t{1}: From {2}\r\n//\t{1}:      [{3}]\r\n//\r\n//\t{1}:   To {4}\r\n//\t{1}:      [{5}]\r\n//\r\n",
											BackupTimeStamp, process,
											ZipFile.DatabaseSchema, ZipFile.FullFilename,
											targetObject.DatabaseSchema, targetObject.Server)); 

			try
			{
				if (zipFile.HasSchemaTables())
				{
					zipFile.Objects("Tables");
					schemaCount = RestoreTableSchema();
					
					if (!(failed = schemaCount <= 0))
						count = RestoreTableObjects();
				}
				else
					VELog.Error("VEAdminZip.RestoreTables: database = " + zipFile.Database + "; schema = " + zipFile.Schema + "; HasNoTables");
			}
			catch (Exception e)
			{
				Error("VEAdminZip.RestoreTables Exception: ", e);
				failed = true;
			}
			finally
			{
				Memo(Abort || failed ? "Error" : "Totals", string.Format("\r\n//\r\n//\t{0}: {1, 7:n0} Of {2:n0} Tables Restored\r\n//\t{0}: {3, 7} {4}\r\n//\r\n",
													process, count, schemaCount, "**",
													Abort ? "Restoration Aborted" : failed ? "Restoration Failed" : VETimeStamp.ElapsedTime(startTime)));

				taskCompletion();

				VELog.Error(string.Format("VEAdminZip.RestoreTables: min = {0}; max = {1}; max1024 = {2}; max2048 = {3} ", minCommand, maxCommand, maxCommand1024, maxCommand2048));
			}
		}

		/// <summary> Restores the table 'veTableDependencies' from CREATE and INSERT scripts in .zipFile  </summary>
		/// <returns></returns>
		protected int RestoreTableSchema()
		{
			zipFile.Objects("Tables");
			xcount = 0;

			return RestoreTable(veTableDependencies) ? RestoreTableData() : -1;
		}

		int RestoreTableObjects()
		{
			string dependenciesTable = TargetSchema + "." + veTableDependencies;
			int count = 0;

			if (targetCmd.Fill(dependenciesTable,
									string.Format("SELECT * FROM {0} ORDER BY Sequence, Name", dependenciesTable)) < 0)
				Memo("Error", string.Format("Restore User Tables: {0, 7} Unable to retrieve new dependencies table - {1}\r\n",
									"ERROR", "", dependenciesTable));
			else
			{
				////
				////	Drop tables in reverse sequence order
				////

				//DataTable dropTable = targetCmd.TableDependencies(targetObject.Schema);

				//if (dropTable != null)
				//    foreach (DataRow row in dropTable.Select("", "Sequence DESC, Name"))
				//    {
				//        string name = ((string)row["Name"]); //.Replace(restoreObject.FileSchema, restoreObject.TableSchema);
				//        Status("Dropping Tables: " + name);
				//        targetCmd.DropTable(name);
				//    }

				DropTables();

				//Status("");

				//
				//	Restore tables in ascending sequence order
				//

				DataTable table = targetCmd[dependenciesTable];

				for (int iRow = count = 0; iRow < table.Rows.Count && !Abort; iRow++)
				{
					string name = ((string)table.Rows[iRow]["Name"]);
					name = name.Substring(name.LastIndexOf('.') + 1);

					if (name.ToLower() == veTableDependencies.ToLower() ||
								name.ToLower() == veTableSchema.ToLower() ||
								name.ToLower() == veTableDefinition.ToLower())
						Memo("Memo", string.Format("Restore User Tables: {0, 7} Table INTENTIONALLY NOT RESTORED - {1}\r\n", "-", TargetSchema + "." + name));

					else if (RestoreTable(name) && RestoreTableData() >= 0)
						count++;

					else
						break;
				}
			}

			return count;
		}

		/// <summary> Retrieves and Executes the script to re-CREATE the table 'name'
		/// <para> . Retrieves the script from the zip file </para>
		/// <para> . Drops the table 'name' if it already exists </para>
		/// <para> . Replaces the schema in script with .TargetSchema </para>
		/// <para> . Sets .restoreIdentity if the script contains its marker </para>
		/// <para> . Verifies that this is a script to CREATE </para>
		/// <para> . Executes the CREATE script </para>
		/// <para> . Verifies that the table 'name' was successfully created </para>
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		bool RestoreTable(string name)
		{
			string tablename = TargetSchema + "." + name,
					text = zipFile.InflateText(name),
					process = "Restore User Tables";

			bool okay = text != null;

			if (!okay)
				Memo("Error", string.Format("{0}: {1, -7} Unable to create {2}\r\n{0}: {3, 7} Unable to Retrieve Script\r\n",
											process, "ERROR", tablename, ""));

			else if (!(okay = targetCmd.DropTable(tablename)))
				Memo("Error", string.Format("{0}: {1, -7} Unable to drop table: {2}", process, "ERROR", tablename));

			else
			{
				try
				{
					if (xcount > 0)
						Memo("Memo", "\r\n***** Original\r\n" + text);

					text = text.Replace(zipFile.Schema, TargetSchema);

					int length = text.Length > 100 ? 100 : text.Length;

					restoreIdentity = text.IndexOf(" IDENTITY ") > 0;

					if (xcount-- > 0)
						Memo("Memo", "\r\n***** Revised: restoreIdentity = " + restoreIdentity.ToString() + "\r\n" + text);

					if (!(okay = text.Substring(0, length).ToLower().IndexOf("create ") >= 0))
						Memo("Error", string.Format("{0}: {1, -7} Unable to create table - {2}\r\n" +
														"{0}: {3, 7} Review the following script for a 'Create Table ' statement -\r\n\r\n{4}\r\n",
															process, "ERROR", tablename, "", text));
					else
					{
						 targetCmd.Execute(text);

					    if (!(okay = targetCmd.TableExists(tablename)))
					        Memo("Error", string.Format("{0}: {1, -7} Unable to create table - {2}\r\n" +
														"{0}: {3, 7} Review the following failed script - {4}\r\n\r\n{5}\r\n",
														process, "ERROR", tablename, "", name, text));
					}
				}
				catch (SystemException e)
				{
					Error("VEAdminZip.RestoreTable:  Attempting to replace schema names", e);
				}
			}

			return okay;
		}

		/// <summary> Retrieves and Executes the script to restore table records
		/// <para> . Retrieves the script from the current position in the zip file </para>
		/// <para> . Parses and executes the script, record by record </para>
		/// <para> . If .restoreIdentity is set, prepends and appends each inserted record text with the necessary SET_IDENTITY_INSERT statements</para>
		/// Returns the number of records restored
		/// </summary>
		/// <returns></returns>
		int RestoreTableData()
		{
			int count = 0, tableCount = 0;

			string	zipSchema = zipFile.Schema + ".",
					targetSchema = TargetSchema + ".",
					tablename = TargetSchema + "." + zipFile.Name,
					process = "Restore User Tables";

			long position = zipFile.Position;

			string[] lines = zipFile.InflateLines();

			if (lines == null)
				Memo("Error", string.Format("{0}: {1, 7} Unable to restore table data - {2}\r\n{3,23} Unable to retrieve data content\r\n",
									process, "ERROR", tablename, ""));

			else
			{
				SQLHandler.Clear();

				try
				{
					string text,
							columnNames = lines[0];			// column names in (ColumnName1,...,) format

					string identityInsertOn = restoreIdentity ? "SET IDENTITY_INSERT " + tablename + " ON " : "",
							identityInsertOff = restoreIdentity && identityInsertOn.Length > 0 ? "SET IDENTITY_INSERT " + tablename + " OFF " : "";

					Status("Restoring Tables: " + tablename);

					for (int i = 1; i < lines.Length && !Abort; )
					{
						Status("Restoring  Tables: " + tablename + " - " + (lines.Length - i).ToString());

						string cmd = "";

						for (; i < lines.Length && cmd.Length < 200000; i++)
						{
							if ((text = lines[i]).Length > 0 && text.IndexOf("System.") < 0)
							{
								text.Replace(zipSchema, targetSchema);
								int ifirst = text.IndexOf('\'');		//	Look for internal single quotes

								if (ifirst >= 0)
								{
									parseQuotes = false;

									for (int paren = RestoreParse(text); paren > 0 && ++i < lines.Length && !Abort; )
									{
										string txt = lines[i];
										paren += RestoreParse(txt);
										text += txt;
									}
								}

								cmd += string.Format("{0} INSERT INTO {1} {2} VALUES {3} {4} \r\n",
										identityInsertOn, tablename, columnNames, text, identityInsertOff);

								count++;
							}
						}

						if (cmd.Length > 0)
						{
							targetCmd.ModifyTable(cmd);

							if (cmd.Length < minCommand)
								minCommand = cmd.Length;

							if (cmd.Length > maxCommand)
								maxCommand = cmd.Length;

							if (cmd.Length < 1025 && cmd.Length > maxCommand1024)
								maxCommand1024 = cmd.Length;

							if (cmd.Length > 1024 && cmd.Length < 2049 && cmd.Length > maxCommand2048)
								maxCommand2048 = cmd.Length;

							Thread.Sleep(25);				// Sleeping avoids unexplained errors.  Probably not handling asynchronous server executions and repsonses properly
						}
					}

					tableCount = targetCmd.TableCount(tablename);

				}
				finally
				{
					Status("");

					Memo(SQLHandler.HasErrors ? "Error" : "Totals", string.Format("{0}{1}: {2, 7:n0} {3}Records restored to {4}{5}",
						SQLHandler.HasErrors ? "\r\n" : "", process, 
						tableCount, tableCount != count ? "Of " + count.ToString() + " " : "", tablename,
						SQLHandler.HasErrors ? " - With Errors\r\n" + SQLHandler.Errors : ""));
				}
			}

			return count;
		}

		#endregion Restore Tables

		#region Restore Procedures and Functions

		/// <summary>Drops all schema procedures and restores all procedures found in scripts within the '[selected path]\Procedures' folder</summary>
		public override void RestoreProcedures() 
		{
			RestoreStoredObjects("Procedures", targetCmd.RetrieveProcedureNames(targetObject.Schema), RestoreDropAndExists[0]);
		}

		/// <summary>Drops all schema functions and restores all functions found in scripts within the '[selected path]\Functions' folder</summary>
		public override void RestoreFunctions() 
		{
			RestoreStoredObjects("Functions", targetCmd.RetrieveFunctionNames(targetObject.Schema), RestoreDropAndExists[1]);
		}

		protected void RestoreStoredObjects(string type, string[]names, VEBooleanDelegateString[] dropAndExists)
		{
			DateTime startTime = DateTime.Now;
			int count = 0, size = 0;
			DataRowCollection rows = null;
			
			string	sourceSchema = zipFile.Schema,
					targetSchema = targetObject.Schema,
					processType = "Restore Schema " + type;

			Memo("Start", string.Format("//\r\n//\t{0}\r\n//\t{1}: From {2}\r\n//\t{1}:      [{3}]\r\n//\r\n//\t{1}:   To {4}\r\n//\t{1}:      [{5}]\r\n//\r\n",
											BackupTimeStamp, processType,
											ZipFile.DatabaseSchema, ZipFile.FullFilename, 
											targetObject.DatabaseSchema, targetObject.Server));

			try
			{
				RestoreDropObjects(type, names, dropAndExists[0]);

				rows = zipFile.Objects(type).Rows;
				
				foreach(DataRow row in rows)
				{
					string	name = (string) row["Name"];

					Status("Restoring " + type + ": " + name);
					
					string ownerObject = targetSchema + "." + name,
							text = zipFile.InflateText(type, name);

					RestoreCreateObject(type, text, sourceSchema, targetSchema, ownerObject, dropAndExists[1], ref size, ref count);
				}
			}
			finally
			{
				Memo("Totals", string.Format("\r\n{0}: {1, 7:n0} Bytes - {2:n0} of {3:n0} {0} Created\r\n{0}: {4, 7} {5}\r\n",
											processType, size, count, rows.Count, "**", Abort ? "Operation Aborted" : VETimeStamp.ElapsedTime(startTime)));
			}
		}

		#endregion Restore Procedures and Functions

		public override void RestoreText(string type, string name)
		{
			Memo("Memo", ZipFile.InflateText(type, name));

			if (type == "Tables")
				Memo("Memo", zipFile.InflateText());
		}

		public override string InflateText(string type, string name) 
		{
			return ZipFile.InflateText(type, name) + (type == "Tables" ? "\r\n/*\r\n**	Data\r\n*/\r\n\r\n" + zipFile.InflateText(type, name, false) : "");  
		}


		#endregion Restore
	}
}
