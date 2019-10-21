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
	public partial class VEAdminFiles : VEDataAdmin
	{
		public VEAdminFiles(VEDataControl datacontrol, VESQLHandlers sqlHandler, VEDelegateStringString memo, VEDelegateText progress, VEDelegateText status)
			: base(datacontrol, sqlHandler, memo, progress, status)
		{ }

		#region Backup

		protected new VEAdminFileName backupObject = new VEAdminFileName();

		protected override string ActiveSchema
		{
			get { return base.ActiveSchema; }
			set
			{
				base.ActiveSchema = value;
				////	Set the working SourceSchema to 'value' or restore the orignal SourceSchema
				//SourceSchema = value;

				//	Set the backup folder hierarchy
				backupLocation.Folder(backupDatabaseFolder, backupDatabaseFolder);
				backupLocation.Folder("Schema", backupSchemaFolder);
				backupLocation.Folder("Tables", backupSchemaFolder + "\\Tables");
				backupLocation.Folder("Data", backupSchemaFolder + "\\Tables\\Data");
				backupLocation.Folder("Procedures", backupSchemaFolder + "\\Procedures");
				backupLocation.Folder("Functions", backupSchemaFolder + "\\Functions");

				//BackupLog.Folder(backupDatabaseFolder, backupDatabaseFolder);
				//backupLog.Folder("Schema", backupSchemaFolder);
				//backupLog.Folder("Tables", backupSchemaFolder + "\\Tables");
				//backupLog.Folder("Data", backupSchemaFolder + "\\Tables\\Data");
				//backupLog.Folder("Procedures", backupSchemaFolder + "\\Procedures");
				//backupLog.Folder("Functions", backupSchemaFolder + "\\Functions");

				//	Prepare backupObject for usage
				backupObject.Set(sourceObject);
				backupObject.Paths = backupLocation;
				//backupObject.Paths = backupLog;
			}
		}

		#region Backup Table Objects

		protected override Totals BackupTables(string schema)
		{
			DateTime startTime = DateTime.Now;
			ActiveSchema = schema;
			string process = "Backup Schema Tables";

			Memo("Start", string.Format("//\r\n//\t{0}\r\n//\t{1}: From {2}\r\n//\t{1}:      [{3}]\r\n//\t{1}:\r\n//\t{1}:   To {4}/r/n//\t{1}:      [{5}]\r\n//\r\n",
										BackupTimeStamp, process, 
										sourceObject.DatabaseSchema, sourceObject.Server, backupObject.Paths["Tables"], backupObject.Server));

			try
			{
				string[] tablenames = sourceCmd.RetrieveTableNames(schema);

				if (tablenames.Length == 0)
					Memo("NONE", string.Format("Backup Schema Tables: {0, 7} There are no Tables to backup", "NONE"));
			
				else if (!(Abort = !BackupTableDependencies()))
				{
					int tableCount = 0, recordCount = 0, count = 0;

					for (int i = 0; i < tablenames.Length && !Abort; i++)
					{
						backupObject.FileName = tablenames[i];

						if (backupObject.ObjectName.ToLower() == veTableDependencies.ToLower() ||
							backupObject.ObjectName.ToLower() == veTableSchema.ToLower() ||
							backupObject.ObjectName.ToLower() == veTableDefinition.ToLower())
							Memo("Memo", string.Format("Backup Schema Tables: {0, 7} Table INTENTIONALLY NOT BACKED UP - ...Data\\{1}", "-", backupObject.SchemaTable));

						else
						{
							count = BackupTableObject(ref recordCount);

							Memo("Memo", string.Format("Backup Schema Tables: {0, 7:n0}{1} Records written to ...Data\\{2}",
								count, count != recordCount ? " ## Of " + recordCount.ToString("N0") : "", backupObject.TableFileName));

							tableCount++;
							BackupTableScript(GetSchemaDefinition(sourceCmd, backupObject.SchemaTable));
						}
					}

					Memo("Totals", string.Format("\r\nBackup Schema Tables: {0, 7:n0} Tables {1} Copied",
											tableCount, tableCount == tablenames.Length ? "" : "of " + tablenames.Length.ToString() + " Tables"));
				}
			}
			catch (Exception e)
			{
				Error("VEAdminFiles.BackupTables: " + schema, e);
			}
			finally
			{
				string text = string.Format("Backup Schema Tables: {0, 7} {1}\r\n", "**", Abort ? "Operation Aborted" : VETimeStamp.ElapsedTime(startTime));

				if (!Abort)
					Memo("Totals", text);
				else
					Error(text);

				ActiveSchema = null;
				Status("");
			}

			return null;
		}

		protected override void BackupTableDependencies(DataTable tableDependencies)
		{
			BackupTableScript(GetSchemaDefinition(tableDependencies));

			backupObject.FileName = veTableDependencies;

			StreamWriter stream = File.CreateText(backupObject.FilePaths["Tables"]);
			BackupTableDependencies(stream, tableDependencies);

			stream.Close();
		}

		protected void BackupTableScript(string text)
		{
			StreamWriter file = File.CreateText(backupObject.FilePaths["Script"]);
			file.WriteLine(text);
			file.Close();
		}

		public override void BackupTable(string tablename)
		{
			Memo("Memo", string.Format("Backup Schema Table: {0}\r\n" +
								"{1, 19}: [{2}].{3}...\r\n",
						sourceObject.ServerIDTableName, "To", backupObject.ServerID, backupObject.Paths["Data"]));

			ActiveSchema = SourceSchema;					//	Required to set up the write paths
			backupObject.FileName = tablename;

			int recordCount = 0,
				count = BackupTableObject(ref recordCount);

			BackupTableScript(GetSchemaDefinition(sourceCmd, backupObject.SchemaTable));

			Memo("Memo", string.Format("Backup Schema Table: {0, 7:n0}{1} Records written to {2}\r\n",
				count, count != recordCount ? " ## Of " + recordCount.ToString("N0") : "", backupObject.TableFileName));
		}

		#endregion Backup Table Objects

		#region Backup Stored Objects

		protected override Totals BackupStoredObjects(string type, string schema)
		{
			DateTime startTime = DateTime.Now;

			ActiveSchema = schema;

			Memo("Memo", string.Format("//\r\n//\t{0}\r\n//\tBackup Schema {1}: From {2}...\r\n" +
								"//\tBackup Schema {1}:   To [{3}].{4}...\r\n//\r\n",
						BackupTimeStamp, type,
						sourceObject.ServerIDDatabaseSchema, backupObject.ServerID, backupObject.Paths[type]));

			Status(string.Format("Backing-up {0} to {1}", type, backupObject.Paths[type]));

			try
			{
				objectCount = objectSize = 0;
				Abort = false;

				string[] objectNames = type == "Procedures" ? sourceCmd.RetrieveUserProcedures(schema)
															: sourceCmd.RetrieveFunctionNames(schema);

				if (objectNames.Length == 0)
					Memo("NONE", string.Format("Backup Schema {0}: {1, 7} There are no {0} to backup", type, "NONE"));
				else
				{
					foreach (string name in objectNames)
						if (name.IndexOf(schema + ".") == 0)
						{
							objectCount++;
							backupObject.ScriptName = name;
							objectSize += BackupStoredObject(type);

							if (Abort)
								break;
						}

					if (objectCount > 0)
						Memo("Memo", string.Format("\r\nBackup Schema {0}: {1, 7:n0} {0} Copied - {2:n0} Bytes", type, objectCount, objectSize));
					else
						Memo("NONE", string.Format("Backup Schema {0}: {1, -7} There are no Schema-qualified {0} to backup", type, "NONE"));
				}

			}
			catch (Exception e)
			{
				Memo("Memo", string.Format("\r\nBackup Schema {0}: {1,7} {2}\r\n", type, "ERROR", e.Message));
			}
			finally
			{
				Memo("Memo", string.Format("Backup Schema {0}: {1, 7} {2}\r\n", type, "**", Abort ? "Operation Aborted" : VETimeStamp.ElapsedTime(startTime)));
				Status("");

				ActiveSchema = null;
			}

			return null;
		}

		public override void BackupProcedure(string name) { BackupSelectedObject("Procedures", name); }
		public override void BackupFunction(string name) { BackupSelectedObject("Functions", name); }

		void BackupSelectedObject(string type, string name)
		{
			ActiveSchema = SourceSchema;
			backupObject.ScriptName = name;

			Memo("Memo", string.Format("Backup Schema {0}: From {1}...\r\n" +
								"Backup Schema {0}:   To [{2}].{3}...\r\n",
								type, SourceServerDatabaseSchema, backupObject.ServerID, backupObject.Paths[type]));

			BackupStoredObject(type);
			Memo("Memo", "");
		}

		#endregion Backup Stored Objects

		#endregion Backup

		#region Restore

		VEAdminFileName restoreObject = new VEAdminFileName();

		public override string RestoreFolder
		{
			get { return restoreObject.Path; }
			set
			{
				base.RestoreFolder = value.Substring(0, value.LastIndexOf('\\')); ;

				restoreObject.Set(targetObject);
				restoreObject.Path = value;
			}
		}

		#region Restore Tables

		public override void RestoreTables(VEDelegate taskCompletion)
		{
			DateTime startTime = DateTime.Now;
			int count = 0, schemaCount = 0;
			bool failed = false;

			Memo("Memo", string.Format("//\r\n//\t{0}\r\n//\tRestore User Tables: From {1}...\r\n" +
								"//\tRestore User Tables:   To {2}...\r\n//\r\n",
						BackupTimeStamp, restoreObject.Tables, targetObject.ServerIDDatabaseSchema)); //.ServerIDDatabaseUser));

			try
			{
				schemaCount = RestoreTableSchema();

				if (!(failed = schemaCount <= 0))
					count = RestoreTableObjects();
			}
			catch (Exception e)
			{
				Memo("Memo", "VEAdmin.RestoreTables.Exception:\r\n\t" + e.Message);
				failed = true;
			}
			finally
			{
				Memo("Memo", string.Format("\r\n//\r\n//\tRestore User Tables: {0, 7:n0} Of {1:n0} Tables Restored" +
						"\r\n//\tRestore User Tables: {2, 7} {3}\r\n//\r\n",
						count, schemaCount, "**",
						Abort ? "Restoration Aborted" : failed ? "Restoration Failed" : VETimeStamp.ElapsedTime(startTime)));

				taskCompletion();
			}
		}

		/// <summary>Returns the number of tables restored from [.FileSchema].veTableDependencies.dat</summary>
		/// <returns></returns>
		int RestoreTableSchema()
		{
			restoreObject.FileName = restoreObject.Schema + "." + veTableDependencies;
			Memo("Memo", "RestoreFileSchema: " + restoreObject.ToString());
			restoreObject.ScriptNameQuirk = !File.Exists(restoreObject.TableScriptPath);

			return RestoreTable() ? RestoreTableData(restoreObject.TablesFilePath) : -1;
		}

		int RestoreTableObjects()
		{
			int count = -1;

			if (targetCmd.Fill(restoreObject.TableName,
									string.Format("SELECT * FROM {0} ORDER BY Sequence, Name", restoreObject.TableName)) < 0)
				Memo("Memo", string.Format("Restore User Tables: {0, 7} Unable to retrieve new dependencies table - {1}\r\n",
									"ERROR", "", restoreObject.TableName));
			else
			{
				DataTable table = targetCmd[restoreObject.TableName];

				//
				//	Drop tables in reverse sequence order
				//

				DataTable dropTable = targetCmd.TableDependencies(targetObject.Schema);

				if (dropTable != null)
					foreach (DataRow row in dropTable.Select("", "Sequence DESC, Name"))
					{
						string name = ((string)row["Name"]).Replace(restoreObject.FileSchema, restoreObject.TableSchema);
						Status("Dropping table " + name);
						targetCmd.DropTable(name);
					}

				Status("");

				//
				//	Restore tables in ascending sequence order
				//

				count = 0;

				for (int iRow = 0; iRow < table.Rows.Count && !Abort; iRow++)
				{
					string name = ((string)table.Rows[iRow]["Name"]);
					restoreObject.FileName = name;

					if (restoreObject.ObjectName.ToLower() == veTableDependencies.ToLower() ||
						restoreObject.ObjectName.ToLower() == veTableSchema.ToLower() ||
						restoreObject.ObjectName.ToLower() == veTableDefinition.ToLower())
						Memo("Memo", string.Format("Restore User Tables: {0, 7} Table INTENTIONALLY NOT RESTORED - {1}\r\n", "-", restoreObject.SchemaTable));

					else if (RestoreTable() && RestoreTableData(restoreObject.DataFilePath) >= 0)
						count++;

					else
						break;
				}
			}

			return count;
		}

		/// <summary>Drops the table specified in restoreObject (.FileName = '[tablename].dat'); 
		/// Restores the table from the script found in restoreObject.TableScriptPath ([selected path]\Tables\[tablename].dat.sql) and;
		/// Returns success or failure in restoring the table specified in restoreObject
		/// </summary>
		/// <returns></returns>
		bool RestoreTable()
		{
			string scriptName = restoreObject.TableScriptName,
					tablename = restoreObject.TableName;

			bool okay = File.Exists(restoreObject.TableScriptPath);

			if (!okay)
				Memo("Memo", string.Format("Restore User Tables: {0, 7} Unable to create table - {1}" +
									"\r\n{23, 7} Script not found - {3}\r\n",
									"ERROR", tablename, "", scriptName));

			else if (!(okay = targetCmd.DropTable(tablename)))
				Memo("Memo", string.Format("Restore User Tables: {0, 7} Unable to drop table: {1}\r\n",
						"ERROR", "", tablename));

			else
			{
				StreamReader scriptFile = File.OpenText(restoreObject.TableScriptPath); //scriptPath);

				try
				{
					string text = scriptFile.ReadToEnd();
					text = text.Replace(restoreObject.FileSchema, restoreObject.TableSchema);

					int length = text.Length > 100 ? 100 : text.Length;

					restoreIdentity = text.IndexOf(" IDENTITY ") > 0;

					if (!(okay = text.Substring(0, length).ToLower().IndexOf("create ") >= 0))
						Memo("Memo", string.Format("Restore User Tables: {0, 7} Unable to create table - {1}" +
											"\r\n{23, 7} Review the following invalid script - {3}\r\n\r\n{4}\r\n",
									"ERROR", tablename, "", scriptName, text));
					else
					{
						targetCmd.Execute(text);

						if (!(okay = targetCmd.TableExists(tablename)))
							Memo("Memo", string.Format("Restore User Tables: {0, 7} Unable to create table - {1}" +
												"\r\n{23, 7} Review the following failed script - {3}\r\n\r\n{4}\r\n",
										"ERROR", tablename, "", scriptName, text));
					}
				}
				finally
				{
					if (scriptFile != null)
						scriptFile.Close();
				}
			}

			return okay;
		}

		/// <summary>Restores table content for the table specified in restoreObject (.FileName = '[tablename].dat'); 
		/// Restores content found in the data file ([selected path]\Tables\Data\[tablename].dat) and;
		/// Returns the number of records restored or -1 if a problem was encountered 
		/// </summary>
		/// <param name="dataPath"></param>
		/// <returns></returns>
		int RestoreTableData(string dataPath)
		{
			int count = -1;

			string dataName = restoreObject.TableFileName,
					tablename = restoreObject.TableName;

			if (!File.Exists(dataPath))
				Memo("Memo", string.Format("Restore User Tables: {0, 7} Unable to restore table data - {1}" +
									"\r\n{23, 7} Data content file not found - {3}\r\n",
									"ERROR", tablename, "", dataPath));

			else
			{
				SQLHandler.Clear();

				StreamReader dataFile = File.OpenText(dataPath);

				try
				{
					string text,
							columns = dataFile.ReadLine();			// column types in (DataType1...) format

					string identityInsertOn = restoreIdentity ? "SET IDENTITY_INSERT " + tablename + " ON " : "",
							identityInsertOff = restoreIdentity && identityInsertOn.Length > 0 ? "SET IDENTITY_INSERT " + tablename + " OFF " : "";

					Status("Restoring " + tablename);

					for (count = 0; !dataFile.EndOfStream && (text = dataFile.ReadLine()).Length > 0 && !Abort; )
						if (text.IndexOf("System.") < 0)
						{
							int ifirst = text.IndexOf('\''),
								ilast = text.LastIndexOf('\'');

							if (ifirst >= 0)
							{
								for (int paren = RestoreParse(text); paren > 0 && !dataFile.EndOfStream && !Abort; )
								{
									string txt = dataFile.ReadLine();
									paren += RestoreParse(txt);
									text += txt;
								}
							}

							targetCmd.ModifyTable(string.Format("{0} INSERT INTO {1} {2} VALUES {3} {4}",
									identityInsertOn, tablename, columns, text, identityInsertOff));

							count++;
						}

					int tableCount = targetCmd.TableCount(restoreObject.TableName);

					Status("");

					Memo("Memo", string.Format("{4}Restore User Tables: {0, 7:n0} {1}Records restored to {2}{3}",
						tableCount, tableCount != count ? "Of " + count.ToString() + " " : "", restoreObject.TableName,
						SQLHandler.HasErrors ? " - With Errors\r\n" + SQLHandler.Errors : "",
						SQLHandler.HasErrors ? "\r\n" : ""));
				}
				finally
				{
					if (dataFile != null)
						dataFile.Close();
				}
			}

			return count;
		}

		#endregion Restore Tables

		#region Restore Procedures and Functions

		/// <summary>Drops all schema procedures and restores all procedures found in scripts within the '[selected path]\Procedures' folder</summary>
		public override void RestoreProcedures() 
		{
			RestoreStoredObjects("Procedures", restoreObject.Procedures, targetCmd.RetrieveProcedureNames(restoreObject.Schema), RestoreDropAndExists[0]);
		}

		/// <summary>Drops all schema functions and restores all functions found in scripts within the '[selected path]\Functions' folder</summary>
		public override void RestoreFunctions() 
		{
			RestoreStoredObjects("Functions", restoreObject.Functions, targetCmd.RetrieveFunctionNames(restoreObject.Schema), RestoreDropAndExists[1]);
		}

		protected void RestoreStoredObjects(string type, string path, string[] names, VEBooleanDelegateString[] dropAndExists)
		{
			DateTime startTime = DateTime.Now;
			StreamReader file = null;

			string [] files = Directory.GetFiles(path, "*.sql");

			int count = 0, size = 0;

			Memo("Memo", string.Format("//\r\n//\t{0}\r\n//\tRestore User {1}: From [Disk].{2}...\r\n" +
								"//\tRestore User {1}:   To {3}...\r\n//\r\n",
						BackupTimeStamp, type, path, targetObject.ServerIDDatabaseSchema));

			try
			{
				RestoreDropObjects(type, names, dropAndExists[0]);

				for (int i = 0; i < files.Length && !Abort; i++)
				{
					string filename = files[i];
					restoreObject.ObjectName = filename.Substring(0, filename.LastIndexOf('.'));

					string ownerObject = restoreObject.SchemaObject;

					file = File.OpenText(filename);

					string text = file.ReadToEnd();

					file.Close();
					file = null;

					RestoreCreateObject(type, text, restoreObject.FileSchema, restoreObject.Schema, restoreObject.SchemaObject, dropAndExists[1], ref size, ref count);

					//int length = text.Length;

					//if (text.Substring(0, length > 100 ? 100 : length).ToLower().IndexOf("create ") >= 0)
					//{
					//    if (restoreObject.FileSchema.Length > 0)
					//        text = text.Replace(restoreObject.FileSchema, restoreObject.Schema).Replace(restoreObject.FileSchema.ToLower(), restoreObject.Schema);

					//    targetCmd.Execute(text);

					//    file.Close();
					//    file = null;

					//    if (dropAndExists[1](ownerObject))
					//    {
					//        Memo(string.Format("Restore User {0}: {1, 7:n0} Bytes - {2} Restored", type, length, ownerObject));
					//        size += length;
					//        count++;
					//    }
					//    else
					//    {
					//        count--;

					//        Memo(string.Format("Restore User {0}: {1, 7} {2} NOT CREATED {3}",
					//                type, "ERROR", ownerObject,
					//                HasSQLErrors ? " - See below\r\n" + SQLErrors : ""));
					//    }
					//}
				}
			}
			finally
			{
				Memo("Memo", string.Format("\r\nRestore User {0}: {1, 7:n0} Bytes - {2:n0} of {3:n0} {0} Created" +
						"\r\nRestore User {0}: {4, 7} {5}\r\n",
						type, size, count, files.Length, "**", Abort ? "Operation Aborted" : VETimeStamp.ElapsedTime(startTime)));

				if (file != null)
					file.Close();
			}
		}

		#endregion Restore Procedures and Functions

		#endregion Restore

	}
}
