using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Text;
using System.Threading;
using System.Windows.Forms;
using Verdugo;
using WebRemote;

namespace VERemoting
{
	public partial class VEDataAdmin : VEDatabase
	{
		protected string backupDatabase;
		protected VEFileLocation backupLocation;
		string backupFolder;

		protected string backupDatabaseFolder { get { return sourceObject.ServerDatabase; } }
		protected string backupSchemaFolder { get { return backupDatabaseFolder + "\\" + SourceSchema; } }
		protected string backupServerDatabase { get { return sourceObject.ServerIDDatabase; } }

		/// <summary> Sets backupSchema to the value provided or restores the original SourceSchema, if 'null', and sets backupLog folders accordingly</summary>
		protected virtual string ActiveSchema 
		{
			get { return backupObject.Schema; } 
			set 
			{
				//	Set the working SourceSchema to 'value' or restore the orignal SourceSchema
				SourceSchema = value;

				//	Prepare backupObject for usage
				backupObject.Set(sourceObject);
				backupObject.Paths = backupLocation;
			} 
		}

		/// <summary> Sets the backup folder to the specified 'value'.  
		/// <para> Returns the specified backup folder or, if not specified, '[.CommonPath]\Backup' where .CommonPath is the data path shared by all application schemas. </para>
		/// (e.g. D:\ProgramData\Verdugo Enterprises\VECharterAdmin\Backup)
		/// </summary>
		public virtual string BackupFolder
		{
			get { return backupFolder != null ? backupFolder : backupFolder = VEFilePaths.CommonPath + "\\Backup"; }
			set
			{
				backupLocation = new VEFileLocation(value, VEFileLocation.Flags.All | VEFileLocation.Flags.FolderOnly);
				backupFolder = value;
			}
		}

		//void CloseVELog(ref VELogFile log)
		//{
		//    if (log != null)
		//    {
		//        log.Close();
		//        log = null;
		//    }
		//}

	}
}
