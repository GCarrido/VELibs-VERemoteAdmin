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
	public delegate void VEDelegateDatatableString (DataTable table, string message);

	/// <summary> Derives from VEDatabase : VECommand : VEConnection : WebRemote.WebConnection : WebResponse
	/// </summary>
	public partial class VEDataAdmin : VEDatabase
	{
		#region Constructors

		public VEDataAdmin(VEDataControl datacontrol, VESQLHandlers sqlHandler, VEDelegateStringString memo, VEDelegateText process, VEDelegateText status)
			: base(datacontrol) 
		{
			SetAdminDatabase(adminDatabase);
			appSQLHandler = sqlHandler;
			Memo = memo;
			Process = process;
			Status = status;
		}

		protected VESQLHandlers appSQLHandler;
		protected VEDelegateStringString Memo;
		protected VEDelegateText Process, Status;

		#region Errors and Exceptions

		protected StringCollection errors = new StringCollection(),
								   exceptions = new StringCollection();

		char errorChar = (char)0x08;

		/// <summary> Calls Memo("Exception", ...) for 'text' and a separate line for each line of text in e.Message using an error marker 
		/// <para> Adds 'text' and e.Message to the .errors collection </para>
		/// </summary>
		/// <param name="text"></param>
		/// <param name="e"></param>
		protected void Error(string text, Exception e)
		{
			string[] messages = e.Message.Split(new string[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);

			Memo("Exception", string.Format("{0}{0}\r\n{0}{0}\tEXCEPTION: {1}", errorChar, text));

			foreach(string message in messages)
				Memo("Exception", string.Format("{0}{0}\tEXCEPTION: {1}", errorChar, message));

			Memo("Exception", string.Format("{0}{0}\r\n", errorChar));

			errors.Add(text + "\r\n" + e.Message);
		}

		/// <summary> Calls Memo("Error", message) using an error marker  
		/// <para> Adds 'message' to the .errors collection </para>
		/// </summary>
		/// <param name="message"></param>
		protected void Error(string message)
		{
			Memo("Error", string.Format("{0}{0}\r\n{0}{0}\t{1}\r\n{0}{0}\r\n", errorChar, message));
			errors.Add(message);
		}

		/// <summary> Calls Memo("Error", ...) with a separate line for each line of text in 'messages' using an error marker  
		/// <para> Adds concatenated 'messages' entries to the .errors collection </para>
		/// </summary>
		/// <param name="messages"></param>
		protected void Error(string[] messages)
		{
			string text = "";

			Memo("Error", string.Format("{0}{0}", errorChar));

			foreach (string message in messages)
			{
				Memo("Error", string.Format("{0}{0}\t{1}", errorChar, message));
				text += message + "\r\n";
			}

			Memo("Error", string.Format("{0}{0}", errorChar));

			if (text.Length > 0)
				errors.Add(text);
		}

		protected void ListErrors()
		{
			if (errors.Count > 0)
			{
				Memo("Error", string.Format("{0}{0}\r\n{0}{0}\t{1:n0} Errors reported:\r\n{0}{0}", errorChar, errors.Count));

				foreach (string error in errors)
					Memo("Error", string.Format("{0}{0}\t\t{1}", errorChar, error.Replace("\r\n", "\r\n" + errorChar + errorChar)));

				Memo("Error", string.Format("{0}{0}", errorChar));
				errors.Clear();
			}
		}

		#endregion Errors and Exceptions

		protected bool abort = false;

		public bool Abort { get { Application.DoEvents(); return abort; } set { abort = value; } }

		#region AdminDatabase

		/// <summary> Set the administrative database in which all pertinent scripts and tables are expected to exist
		/// </summary>
		static public string AdminDatabase { set { adminDatabase = value; } }

		/// <summary> "Gerald_Administration" </summary>
		static string adminDatabase = "Gerald_Administration";

		#endregion AdminDatabase

		void SetAdminDatabase(string database)
		{
			ChangeDatabase(database);
		}

		#endregion Constructors

		public new void Dispose()
		{
			//CloseBackupLogs();
			//CloseRestoreLog();

			base.Dispose();
		}

		#region Utility Methods - ElapsedTime

		//protected string ElapsedTime(DateTime startTime)
		//{
		//    return ElapsedTime(new TimeSpan(DateTime.Now.Ticks - startTime.Ticks));
		//}

		//protected string ElapsedTime(TimeSpan span)
		//{
		//    return string.Format("{0}{1} Minutes {2,2}.{3:D3} Seconds",
		//        span.Hours > 0 ? span.Hours.ToString() + " Hours  " : "", span.Minutes, span.Seconds, span.Milliseconds);
		//}

		#endregion Utility Methods - ElapsedTime

		#region Source and Target objects

		protected VEDatabase sourceCmd, targetCmd;

		/// <summary>Accessor for sourceCmd.  Set also sets sourceObject.DataConnection.</summary>
		protected VEDatabase SourceCmd 
		{ 
			get { return sourceCmd; }
			set 
			{
				if (value != null && value != sourceCmd)				//	Avoid changing .Schema
					sourceObject.DataConnection = sourceCmd = value; 
			} 
		}

		/// <summary>Accessor for targetCmd.  Set also sets targetObject.DataConnection.</summary>
		protected VEDatabase TargetCmd 
		{ 
			get { return targetCmd; } 
			set 
			{
				if (value != null && value != targetCmd)				//	Avoid changing .Schema
					targetObject.DataConnection = targetCmd = value; 
			} 
		}

		/// <summary>Sets a returns the source VEDatabase object 'cmd' </summary>
		/// <param name="cmd"></param>
		public VEDatabase SetSource(VEDatabase cmd) { return SourceCmd = cmd; }

		/// <summary>Sets source names</summary>
		/// <param name="database"></param>
		/// <param name="schema"></param>
		public void SetSource(string database, string schema)
		{
			SourceDatabase = sourceDatabase = database;
			SourceSchema = sourceSchema = schema;
		}

		/// <summary>Sets the target VEDatabase object</summary>
		/// <param name="cmd"></param>
		public VEDatabase SetTarget(VEDatabase cmd) { return TargetCmd = cmd; }

		/// <summary>Sets target names</summary>
		/// <param name="database"></param>
		/// <param name="schema"></param>
		public void SetTarget(string database, string schema)
		{
			TargetDatabase = database;
			TargetSchema = schema;
		}

		#endregion Source and Target VEDatabase objects

		protected VECompress zipFile;
		protected VECompress ZipFile { get { return zipFile != null ? zipFile : zipFile = new VECompress(Memo); } }

		public string ZipFileName { get { return zipFile != null ? zipFile.FullFilename : null; } }
		public bool HasZipFile { get { return zipFile != null; } }
				
		new public string Message { get { return message; } }

		#region DropProcedure and DropFunction

		public void DropProcedure(VEDatabase cmd, string procedure)
		{
			if (cmd.StoredProcedureExists(procedure))
				cmd.DropProcedure(procedure);
		}

		public void DropFunction(VEDatabase cmd, string function)
		{
			if (cmd.StoredFunctionExists(function))
				cmd.DropFunction(function);
		}

		#endregion DropProcedure and DropFunction

		#region ProcedureExists and Parameters

		/// <summary> Returns test results for the existence of stored procedure 'procedure' on 'cmd' connection's database in use.
		/// <para> If 'procedure' does not exist, an attempt is made to create it from this connection's database. </para>
		/// <para> Error messages are stored in .Message </para>
		/// </summary>
		/// <param name="cmd"></param>
		/// <param name="procedure"></param>
		/// <returns></returns>
		public bool ProcedureExists(VEDatabase cmd, string procedure)
		{
			bool exists = cmd.StoredProcedureExists(procedure);

			if (!exists)
			{
				string text = RetrieveProcedureTextString(procedure);

				if (text.Length == 0)
					message = string.Format("VEDataAdmin.ProcedureExists: Unable to create procedure '{0}' from this connection's database [{1}]", procedure, DataControl.Connection);
				else
				{
					cmd.Execute(text);

					if (!(exists = cmd.StoredProcedureExists(procedure)))
						message = string.Format("VEDataAdminProcedureExists: An error occurred trying to create procedure '{0}' on {1}", procedure, cmd.DataControl.Connection);
				}
			}

			return exists;
		}

		/// <summary> Returns the procedure's parameters after creating the stored procedure on 'cmd' if needed
		/// </summary>
		/// <param name="cmd"></param>
		/// <param name="procedure"></param>
		/// <returns></returns>
		public VEDataParameters Parameters(VEDatabase cmd, string procedure)
		{
			return ProcedureExists(cmd, procedure) ? cmd.RetrieveProcedureParameters(procedure) : null;
		}

		#endregion ProcedureExists and Parameters

		#region SQL Error Handling

		protected VESQLHandlers SQLHandler = new VESQLHandlers();
		
		//VEMessages sqlMessages;
		//VEMessages SQLMessages
		//{
		//    get { return sqlMessages != null ? sqlMessages : sqlMessages = new VEMessages(); }
		//}

		//protected VEBooleanDelegateIntString sqlHandler;

		//public VEBooleanDelegateIntString SQLHandler(string type)
		//{
		//    SQLMessages.Text = type;

		//    return SQLHandler();
		//}

		//public VEBooleanDelegateIntString SQLHandler()
		//{
		//    sqlTryAgain = false;
		//    sqlIdentity = true;
		//    SQLMessages.Clear();

		//    return sqlHandler != null ? sqlHandler : sqlHandler = new VEBooleanDelegateIntString(SQLErrorHandler); 
		//}

		//bool sqlTryAgain = false, sqlIdentity = false;

		///// <summary> Gets and Sets .sqlTryAgain.
		///// <para> .SQLHandler() sets it to false and </para>
		///// <para> .SQLErrorHandler(...) sets it to true when 'SET IDENTIFIER ON/OFF' is attempted on a table that does not have an identity property. </para>
		///// </summary>
		//public bool SQLTryAgain { get { return sqlTryAgain; } set { sqlTryAgain = value; } }

		///// <summary> Get:  Returns .identity 
		///// <para> If a SQL error occurred because an attempt was made to 'SET IDENTIFIER ...' on a table that does not have an identity property, </para>
		///// <para> .Handler sets .identity to false to indicate that 'SET IDENTIFIER ...' should be removed </para>
		///// </summary>
		//public bool SQLIdentity { get { return sqlIdentity; } }

		///// <summary> Gets test results for .SQLMessages.Count > 0.
		///// <para> .SQLHandler() sets it to zero and </para>
		///// <para> .SQLErrorHandler(...) increments it for every SQL exception it does not handle as the exception message is stored to .SQLMessages. </para>
		///// <para> Retrieve messages with .SQLErrors. </para>
		///// </summary>
		//public bool HasSQLErrors { get { return SQLMessages.Count > 0; } }

		///// <summary> Gets .SQLMessages.ToString() </summary>
		//public string SQLErrors { get { return SQLMessages.ToString(); } }

		//public void ClearSQLErrors() { SQLMessages.Clear(); }

		///// <summary>  </summary>
		///// <param name="code"></param>
		///// <param name="message"></param>
		///// <returns></returns>
		//bool SQLErrorHandler(int code, string message)
		//{
		//    bool handled = false;

		//    if (!sqlTryAgain && code == 8106)		//	Table '...' does not have the identity property. Cannot perform SET operation.
		//    {
		//        sqlTryAgain = true;
		//        sqlIdentity = false;
		//        handled = true;
		//    }

		//    else
		//    {
		//        handled = code == 2760 ||			//	The specified schema name "Gerald_Admin" either does not exist or you do not have permission to use it.
		//                    code == 1767 ||			//	Foreign key 'FK...' references invalid table '...'.
		//                    code == 1750 ||			//	Could not create constraint. See previous errors.
		//                    code == 547 ||			//	INSERT statement conflicted with COLUMN FOREIGN KEY constraint ...
		//                    code == 257 ||			//	Implicit conversion from data type datetime to int is not allowed...
		//                    code == 271 ||			//	Column ... cannot be modified because it is a computed column
		//                    code == 207 ||			//	Incorrect syntax near '...'
		//                    code == 156 ||			//	Invalid column name ... (e.g. 'Ret Earn') for copying tables and procedures
		//                    code == 105 ||			//	Unclosed quotation mark after the character string '...'
		//                    code == 102;			//	Incorrect syntax near the keyword '...'

		//        sqlTryAgain = false;
		//        sqlIdentity = true;
		//        SQLMessages.Message = handled ? code.ToString() + " - " + message.Substring(0, message.IndexOf('\r')) : message;
		//    }

		//    return !handled;
		//}

		#endregion SQL Error Handling

	}
}
