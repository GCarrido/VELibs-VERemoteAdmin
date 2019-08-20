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
		protected VEObjectName sourceObject = new VEObjectName(),
								targetObject = new VEObjectName();

		protected VEAdminFileName backupObject = new VEAdminFileName();

		/// <summary>Returns .sourceObject</summary>
		protected VEObjectName sourceTable { get { return sourceObject; } }

		/// <summary>Returns .targetObject</summary>
		protected VEObjectName targetTable { get { return targetObject; } }

		protected string CopySourceTable { get { return sourceTable.DatabaseSchemaTable; } }
		protected string CopyTargetTable { get { return targetTable.DatabaseSchemaTable; } } 

		/// <summary>Accessors for sourceObject.Schema. 
		/// The Set accessor restores the original source schema if 'value' is null</summary>
		protected string SourceSchema 
		{ 
			get { return sourceObject.Schema; } set { sourceObject.Schema = value != null ? value : sourceSchema; } 
		}

		string sourceSchema, sourceDatabase;

		/// <summary>Gets: .sourceObject.Database.  
		/// <para> Sets:  </para>
		/// <para> . sourceObject.Database and .backupDatabase to 'value', restoring the original .sourceDatabase if 'value' is null </para>
		/// <para> . the database in use to .backupDatabase </para>
		/// </summary>
		protected string SourceDatabase 
		{
			get { return sourceObject.Database; } 
			set { SourceCmd.ChangeDatabase(sourceObject.Database = backupDatabase = value != null ? value : sourceDatabase); } 
		}

		/// <summary>Accessors for targetObject.Schema</summary>
		protected string TargetSchema { get { return targetObject.Schema; } set { targetObject.Schema = value; } }

		/// <summary>Accessors for targetObject.Database.  The Set accessor also calls TargetCmd.ChangeDatabase('value')</summary>
		protected string TargetDatabase 
		{ 
			get { return targetObject.Database; } 
			set { TargetCmd.ChangeDatabase(targetObject.Database = value); } 
		}

		/// <summary>Returns '[sourceServerID].sourceDatabase'</summary>
		protected string SourceServerDatabase { get { return sourceObject.ServerIDDatabase; } }

		/// <summary>Returns '[sourceServerID].sourceDatabase.sourceSchema'</summary>
		protected string SourceServerDatabaseSchema { get { return sourceObject.ServerIDDatabaseSchema; } }

		/// <summary>Returns '[targetServerID].targetDatabase.targetSchema'</summary>
		protected string TargetServerDatabaseSchema { get { return targetObject.ServerIDDatabaseSchema; } }

	}
}
