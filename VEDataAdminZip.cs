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
		public VEAdminZip(VEDataControl datacontrol, VESQLHandlers sqlHandler, VEDelegateStringString memo, VEDelegateText progress, VEDelegateText status)
			: base(datacontrol, sqlHandler, memo, progress, status)
		{
		}

		#region BackupFolder

		/// <summary> Gets .BackupFolder as set or '[.CommonPath]\Backup' if not set. (e.g. [D:\ProgramData\Verdugo Enterprises\VECharterAdmin]\Backup)
		/// Sets .BackupFolder to 'value.  Also establishes the root folder for compressed file operations
		/// </summary>
		public override string BackupFolder
		{
			get { return base.BackupFolder; }
			set
			{
				base.BackupFolder = value;
				VELog.Error("VEAdminZip.BackupFolder: " + value);
				ZipFile.SetDataBasePath(value);
			}
		}

		#endregion BackupFolder

		#region RestoreFolder

		public override string RestoreFolder
		{
			get { return base.RestoreFolder; }
			set
			{
				string baseFolder = value.Substring(0, value.LastIndexOf('\\')); ;
				base.RestoreFolder = baseFolder.Substring(0, baseFolder.LastIndexOf('\\')); ;
				ZipFile.Open(value);
			}
		}

		public override string RestoreFile
		{
			get { return base.RestoreFile; }
			set
			{
				base.RestoreFile = value;
				ZipFile.Open(value);
			}
		}

		#endregion RestoreFolder


	}
}
