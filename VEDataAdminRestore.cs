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
	/// <summary> Derives from VEObjectName
	/// <para> Supplements VEObjectName's naming conventions for RDMS system objects with that for data files.</para>
	/// </summary>
	public class VEAdminFileName : VEObjectName
	{
		Dictionary<string, string> filePaths;

		public Dictionary<string, string> FilePaths
		{
			get
			{
				if (filePaths == null)
				{
					filePaths = new Dictionary<string, string>();
					filePaths.Add("Functions", "");
					filePaths.Add("Procedures", "");
					filePaths.Add("Tables", "");
					filePaths.Add("Data", "");
				}

				return filePaths;
			}
		}

		VEFolder paths;
		public VEFolder Paths
		{
			get { return paths; }
			set { Path = (paths = value)["Schema"] != null ? paths["Schema"] : paths.FilePath; }
		}

		/// <summary> Accessors for a root folder's path which concludes with the original schema(e.g. '...\Gerald_Admin'.  
		/// The Set accessor sets .FileSchema to the original schema, .ServerID to 'Disk', .Database to the path provided, and
		/// sets .Paths to 'Tables', 'Data', 'Procedures' and 'Functions' folders
		/// </summary>
		public string Path
		{
			get { return Database; }
			set
			{
				Database = value;
				ServerID = (new DriveInfo(value)).DriveType.ToString() + " Drive";
				
				int iDot = value.LastIndexOf('.'),
					iSlash = value.LastIndexOf('\\');

				fileSchema = iDot > 0 && iDot > iSlash ? value.Substring(iDot + 1)
							 : iSlash > 0 ? value.Substring(iSlash + 1) : "";
			}
		}

		string fileSchema = "", fileName = "";

		public string IDPath { get { return string.Format("[0].{1}", ServerID, Path); } }

		/// <summary>Accessor for a tables's original schema file name (e.g. 'Gerald_Admin.Employees').
		/// The .Set accessor sets .Filename to the specified name appended with '.dat' (e.g. 'Gerald_Admin.Employees.dat'),
		/// .objectname to the object's name (e.g. 'Employees') and
		/// .FilePaths to full file paths for 'Tables', 'Data', 'Procedures', 'Functions' and 'Script' files.
		/// The current schema is used if not provided in 'value'</summary>
		public string FileName
		{
			get { return fileName; }
			set { SetFileName(value, ".dat");  ScriptNameQuirk = false; }
		}

		/// <summary>Accessor for a stored object's original schema file name (e.g. 'Gerald_Admin.CopyTable').
		/// The .Set accessor set's .ScriptName to the specified name appended with '.sql' (e.g. 'Gerald_Admin.CopyTable.sql') and
		/// .objectname to the object's name (e.g. 'CopyTable') and
		/// .FilePaths to full file paths for 'Tables', 'Data', 'Procedures', 'Functions' and 'Script' files.
		/// The current schema is used if not provided in 'value'</summary>
		public string ScriptName
		{
			get { return fileName; }
			set{ SetFileName(value, ".sql"); }
		}

		void SetFileName(string name, string extension)
		{
			if ((fileName = name).IndexOf('.') < 0)
				fileName = Schema + "." + fileName;

			if (fileName.ToLower().LastIndexOf(extension) != name.Length - 4)
					fileName += extension;

			int iFirst = fileName.IndexOf('.'),
				iLast = fileName.LastIndexOf('.');

			objectname = iLast > iFirst ? fileName.Substring(iFirst + 1, iLast - iFirst - 1)
						: fileName.Substring(0, iFirst);

			FilePaths["Data"] = Paths["Data"] + "\\" + fileName;
			FilePaths["Tables"] = Paths["Tables"] + "\\" + fileName;
			FilePaths["Procedures"] = Paths["Procedures"] + "\\" + fileName;
			FilePaths["Functions"] = Paths["Functions"] + "\\" + fileName;
			FilePaths["Script"] = Paths["Tables"] + "\\" + TableScriptName;
		}

		/// <summary>The full specified .Path appended with a 'Tables' folder (e.g. '...\2009-01-31\IX HOST...\Gerald_Admin\Tables'</summary>
		public string Tables { get { return Paths["Tables"]; } } // Database + "\\Tables"; } }

		/// <summary>The full 'Tables' path appended with a 'Data' folder (e.g. '...\2009-01-31\IX HOST...\Gerald_Admin\Tables\Data'</summary>
		public string Data { get { return Paths["Data"]; } } // Tables + "\\Data"; } }

		/// <summary>The full specified .Path appended with a 'Procedures' folder (e.g. '...\2009-01-31\IX HOST...\Gerald_Admin\Procedures'</summary>
		public string Procedures { get { return Paths["Procedures"]; } } // Database + "\\Procedures"; } }

		/// <summary>The full specified .Path appended with a 'Functions' folder (e.g. '...\2009-01-31\IX HOST...\Gerald_Admin\Functions'</summary>
		public string Functions { get { return Paths["Functions"]; } } // Database + "\\Functions"; } }
		
		/// <summary>The schema name to which objects to be restored originally belonged</summary>
		public string FileSchema { get { return fileSchema; } }
		public string OwnerFile { get { return fileSchema + "." + objectname; } }

		public string TablesPath { get { return Tables + "\\" + fileName; } }
		public string TablesOwnerPath { get { return Tables + "\\" + OwnerFile; } }
		public string TablesOwnerSQLPath { get { return TablesOwnerPath + ".sql"; } }

		public string DataPath { get { return Data + "\\" + fileName; } }
		public string DataOwnerPath { get { return Data + "\\" + OwnerFile; } }
		public string DataOwnerSQLPath { get { return DataOwnerPath + ".sql"; } }

		public string ProcedurePath { get { return Procedures + "\\" + fileName; } }
		public string FunctionPath { get { return Functions + "\\" + fileName; } }

		/// <summary>The schema name to which original objects are to be restored</summary>
		public string TableSchema { get { return Schema; } }

		/// <summary>Returns the new schema's table name which will be recreated and to which the original schema's data will be restored</summary>
		public new string TableName { get { return SchemaObject; } }

		/// <summary>Returns the name of the data file containing the original table's content (e.g. 'Gerald_Admin.Employees.dat')</summary>
		public string TableFileName { get { return fileName; } }

		/// <summary>Returns the full 'Tables' path of the data file containing the original table's content (e.g. '...\Tables\veTableDependencies.dat')</summary>
		public string TablesFilePath { get { return Tables + "\\" + fileName; } }

		/// <summary>Returns the full 'Data' path of the data file containing the original table's content (e.g. '...\Tables\Data\Gerald_Admin.Employees.dat')</summary>
		public string DataFilePath { get { return Data + "\\" + fileName; } }

		public bool ScriptNameQuirk = false;

		/// <summary>Returns the file name containing the script to re-create the original table object (e.g. 'Gerald_Admin.Employees.dat.sql')</summary>
		public string TableScriptName { get { return (ScriptNameQuirk ? FileSchema + "." : "") + FileName + ".sql"; } }

		/// <summary>Returns the full path name containing the script to re-create the original table object (e.g. '...\Tables\Gerald_Admin.Employees.dat.sql')</summary>
		public string TableScriptPath { get { return Tables + "\\" + TableScriptName; } }

		public new string ToString()
		{
			return string.Format("{0}\r\n{1,15}: {2}\r\n{3,15}: {4}\r\n{5,15}: {6}\r\n{7,15}: {8}\r\n{9,15}: {10}\r\n" +
								 "{11,15}: {12}\r\n{13,15}: {14}\r\n", 
							base.ToString(),
							"Path", Path,
							"TablesFilePath", TablesFilePath,
							"DataFilePath", DataFilePath,
							"TableScriptPath", TableScriptPath,
							"SchemaTable", SchemaTable,
							"Procedures", Procedures,
							"Functions", Functions
							);
		}
	}

	public partial class VEDataAdmin : VEDatabase
	{

		#region RestoreLog

		string restoreFolder, restoreFile;

		/// <summary>Accessors for the location of backed up files.
		/// <para> . For compressed files, point to a compressed backup file with an extension of .vzip, .dzip or .gzip.   </para>
		/// <para> . For uncompressed files, point to a schema-named folder containing subfolders  .\Functions, .\Procedures and .\Tables.   </para>
		/// In either case, the compressed file or uncompressed files will be opened and prepped to .RestoreDatabases(...), .RestoreSchemas(..) and .RestoreObjects(..).
		/// </summary>
		public virtual string RestoreFolder
		{
			// For uncompressed files point to a schema backup folder containing object folders .\Functions, .\Procedures and .\Tables.
			get { return restoreFolder; }
			set { restoreFolder = value; }
		}

		/// <summary>Accessors for the compressed backed up file to be restored with an extension of .vzip, .dzip or .gzip.</summary>
		public virtual string RestoreFile
		{
			get { return restoreFile; }
			set 
			{
				string baseFolder = value.Substring(0, value.LastIndexOf('\\')); ;
				RestoreFolder = baseFolder.Substring(0, baseFolder.LastIndexOf('\\'));
				restoreFile = value;
			}
		}

		#endregion RestoreLog

		/// <summary>Restores all schema tables, procedures and functions from content found in the specified restoration folder</summary>
		public void RestoreSchema(VEDelegate taskCompletion)
		{
			DateTime startTime = DateTime.Now;

			Memo("Memo", string.Format("/*\r\n**\t{0}\r\n**\tRestore Schema {1} - All Objects\r\n*/\r\n",
										BackupTimeStamp, targetObject.DatabaseSchema));

			DropTables();

			if (!Abort)
				RestoreProcedures();

			if (!Abort)
				RestoreFunctions();

			if (!Abort)
				RestoreTables(taskCompletion);

			Memo("Memo", string.Format("/*\r\n**\tRestore Schema {0} - All  Objects: {1}\r\n*/\r\n",
									targetObject.DatabaseSchema, abort ? "ABORTED" : VETimeStamp.ElapsedTime(startTime)));
		}

		#region Restore Tables

		/// <summary>Drops all schema tables and restores all tables specified in schema table '[selected path]\Tables\.veTableDependencies.dat'.
		/// Scripts for each table specified in the schema table must be found in the '[selected path]\Tables' folder to restore the table and
		/// a corresponding data content '.dat' file must be found in '[selected path]\Tables\Data' to repopulate the restored table</summary>
		public virtual void RestoreTables(VEDelegate taskCompletion) { }

		/// <summary>  Drops all tables from .targetObject.Schema </summary>
		protected void DropTables() 
		{
			//
			//	Drop tables in reverse sequence order
			//

			DataTable dropTable = targetCmd.TableDependencies(targetObject.Schema);

			if (dropTable != null)
				foreach (DataRow row in dropTable.Select("", "Sequence DESC, Name"))
				{
					string name = ((string)row["Name"]); //.Replace(restoreObject.FileSchema, restoreObject.TableSchema);
					Status("Dropping Tables: " + name);
					targetCmd.DropTable(name);
				}

			Status("");
		}

		protected string 
			veTableDependencies = "veTableDependencies",
			veTableSchema = "veTableSchema",
			veTableDefinition = "veTableDefinition";

		protected bool restoreIdentity = false;
		protected int xcount = 0;

		protected DataTable tableDependencies;

		protected bool parseQuotes;

		/// <summary> Verifies that 'text' contains matching open- and close-parentheses, if any </summary>
		/// <param name="text"></param>
		/// <returns></returns>
		protected int RestoreParse(string text)
		{
			int paren = 0;
			

			foreach (char c in text)
			{
				if (c == '\'')
					parseQuotes = !parseQuotes;

				if(!parseQuotes)
					if (c == '(')
						paren++;
					else if (c == ')')
						paren--;
			}

			return paren;
		}

		#endregion Restore Tables

		#region Restore Procedures and Functions

		protected VEBooleanDelegateString[][] restoreDropAndExists;
		protected VEBooleanDelegateString[][] RestoreDropAndExists
		{
			get
			{
				if (restoreDropAndExists == null)
				{
					restoreDropAndExists = new VEBooleanDelegateString[2][]
					{
						new VEBooleanDelegateString[] { targetCmd.DropProcedure, targetCmd.StoredProcedureExists},
						new VEBooleanDelegateString[] { targetCmd.DropFunction,  targetCmd.StoredFunctionExists} 
					};
				}

				return restoreDropAndExists;
			}
		}

		/// <summary>Drops all schema procedures and restores all procedures found in scripts within the '[selected path]\Procedures' folder</summary>
		public virtual void RestoreProcedures() 
		{
		}

		/// <summary>Drops all schema functions and restores all functions found in scripts within the '[selected path]\Functions' folder</summary>
		public virtual void RestoreFunctions() 
		{
		}

		protected void RestoreDropObjects(string type, string[] names, VEBooleanDelegateString dropAndExists)
		{
			foreach (string name in names)
			{
				Status("Dropping " +type + ": " + name);
				dropAndExists(name);
			}

			Status("");
		}

		protected void RestoreCreateObject(string type, string text, string sourceSchema, string targetSchema, string ownerObject, VEBooleanDelegateString dropAndExists, ref int size, ref int count)
		{
			int length = text.Length;

			if (text.Substring(0, length > 100 ? 100 : length).ToLower().IndexOf("create ") >= 0)
			{
				if (sourceSchema.Length > 0)
					text = text.Replace(sourceSchema, targetSchema).Replace(sourceSchema.ToLower(), targetSchema);

				targetCmd.Execute(text);

				if (dropAndExists(ownerObject))
				{
					Memo("Memo", string.Format("Restore Schema {0}: {1, 7:n0} Bytes - {2} Restored", type, length, ownerObject));
					size += length;
					count++;
				}
				else
					Memo("Memo", string.Format("Restore Schema {0}: {1, 7} {2} NOT CREATED {3}",
							type, "ERROR", ownerObject,
							SQLHandler.HasErrors ? " - See below\r\n" + SQLHandler.Errors : ""));
			}
		}

		#endregion Restore Procedures and Functions

		#region DataTable objects

		/// <summary>Returns a table of databases found in the selected zip file</summary>
		public virtual DataTable RestoreDatabases { get { return null; } }

		/// <summary>Returns a table of schemas associated with the database indexed by 'dbIndex'.  See .RestoreDatabases()</summary>
		/// <param name="dbIndex"></param>
		/// <returns></returns>
		public virtual DataTable RestoreSchemas(int dbIndex) { return null; }

		/// <summary>Returns a table of 'type' objects (i.e. Tables, Procedures, Functions) associated with the schema indexed by 'sIndex'.  See .RestoreSchemas(...)</summary>
		/// <param name="sIndex"></param>
		/// <param name="type"></param>
		/// <returns></returns>
		public virtual DataTable RestoreObjects(int sIndex, string type) { return null; }

		public string DatabaseSchema { get { return zipFile.DatabaseSchema; } }

		#endregion DataTable objects

		public virtual string RestoreServer { get { return ""; } }
		public virtual string RestoreDatabase { get { return ""; } }

		public virtual void RestoreText(string type, string name) { }
		public virtual string InflateText(string type, string name) { return "";  }


	}
}
