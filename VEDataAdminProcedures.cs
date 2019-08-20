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
		/// <summary> Methods and properties for copying stored procedures and functions
		/// </summary>

		protected int objectCount = 0, objectSize = 0;

		static string insert_new_owner_text = "insert_new_owner_text";

		#region public CopyProcedures and CopyFunctions

		public void CopyProcedures() { CopyObjects(true, sourceCmd.RetrieveProcedureNames(SourceSchema)); }

		public void CopyFunctions() { CopyObjects(false, sourceCmd.RetrieveFunctionNames(SourceSchema)); }

		void CopyObjects(bool isProcedure, string[] objectNames)
		{
			DateTime startTime = DateTime.Now;

			string	text = isProcedure ? "Procedure" : "Function",
					objectName = "", 
					proccessType = "Copy Schema " + text;

			Memo("Start", string.Format("{0}s: From {1}\r\n{0}s:      [{2}]\r\n{0}s:\r\n{0}s:   To {3}\r\n{0}s:      [{4}]\r\n",
										proccessType, sourceObject.DatabaseSchema, sourceObject.Server, targetObject.DatabaseSchema, targetObject.Server));

			try
			{
				objectCount = objectSize = 0;
				Abort = false;

				for (int i = 0; i < objectNames.Length && !Abort; i++)
				{
					sourceObject.ObjectName = objectName = objectNames[i];

					if (objectName.IndexOf(SourceSchema + ".") != 0)
						Memo("Error", string.Format("{0}s: {1, 7} NOT COPIED - {2}", proccessType, "SKIPPED", sourceObject.ServerIDObjectName)); 

					else
					{
						int length = isProcedure ? CopyProcedureObject() : CopyFunctionObject();

						if (length > 0)
						{
							objectSize += length;
							objectCount++;
						}
						else
							Error(string.Format("{0}s: {1, 7} NOT RETRIEVED - {2}", proccessType, "ERROR", sourceObject.ServerIDObjectName)); 
					}
				}
			}
			catch (Exception e)
			{
				Error("VEDataAdmin.CopyObjects: " + text + " - " + objectName, e);
			}
			finally
			{
				text = string.Format("\r\n{0}s: {2, 7:n0} {3}{1}s Copied ({4:n0} Bytes) Of {5} \r\n{0}s: {6, -5} {7}\r\n",
									proccessType,
									text,
									objectCount,
									objectCount == objectNames.Length ? "" : "## ",
									objectSize,
									objectNames.Length,
									"**", Abort ? "Operation Aborted" : VETimeStamp.ElapsedTime(startTime));

				if (!Abort)
					Memo("Totals", text);
				else
					Error(text);

				Status("");
			}
		}

		#endregion CopyProcedures and CopyFunctions

		#region public CopyProcedure and CopyFunction

		public void CopyProcedure(string procedure) { CopyObject(true, procedure); }

		public void CopyFunction(string function) { CopyObject(false, function); }

		void CopyObject(bool isProcedure, string objectName)
		{
			sourceObject.ObjectName = objectName;

			Memo("Memo", string.Format("Copy Schema {0}:         From {1}",
				isProcedure ? "Procedure" : "Function", sourceObject.ServerIDObjectName)); 

			try 
			{
				if (isProcedure)
					CopyProcedureObject();
				else
					CopyFunctionObject();
			}
			finally
			{
				Memo("Memo", "");
			}
		}

		#endregion CopyProcedure and CopyFunction

		#region private CopyProcedureObject and CopyFunctionObject

		/// <summary> Copy a procedure from one server to another server.
		/// The source object must have been initialized using sourceObject.ObjectName = someProcedureName;
		/// </summary>
		/// <returns></returns>
		int CopyProcedureObject() { return CopyObject("Procedure", "P"); }

		/// <summary> Copy a function from a one server to another server.
		/// The source object must have been initialized using sourceObject.ObjectName = someFunctionName;
		/// </summary>
		/// <returns></returns>
		int CopyFunctionObject() { return CopyObject("Function", "FN"); }

		int CopyObject(string text, string type)
		{
			targetObject.ObjectName = sourceObject.ObjectName; 

			Status(string.Format("Copying {0}: {1}", text, targetObject.SchemaObject));

			SQLHandler.Clear();

			int size = CopyObject(type);

			if (SQLHandler.HasErrors)
				Memo("Error", string.Format("Copy Schema {0}s: {1, 7} {2} WAS NOT Copied because of the following SQL Errors\r\n\r\n{3}",
							text, "ERROR", targetObject.ServerIDObjectName, SQLHandler.Errors));
			else
				Memo("Memo", string.Format("Copy Schema {0}s: {1, 7:n0} Bytes   {2}", text, size, targetObject.ObjectName)); 

			return size;
		}

		int CopyObject(string type)
		{
			string objectname = targetObject.ObjectName; 

			if (type == "P")
				DropProcedure(targetCmd, targetObject.DatabaseSchemaObject); 
			else
				DropFunction(targetCmd, targetObject.SchemaFunction); 

			string text = sourceCmd.RetrieveProcedureTextString(sourceObject.SchemaObject).Replace(insert_new_owner_text, TargetSchema);
			
			int i = text.Length, 
				j = 0;
			
			string lowerText = text.Substring(0, i > 100 ? 100 : i).ToLower();

			i = lowerText.IndexOf("." + objectname.ToLower());
			j = lowerText.IndexOf(objectname.ToLower());

			if (i < 0 || i > j)
				text = text.Substring(0, j) + TargetSchema + "." + text.Substring(j);
			
			targetCmd.Execute(text = text.Replace(SourceSchema, TargetSchema).Replace(SourceSchema.ToLower(), TargetSchema));

			return text.Length;
		}

		#endregion CopyProcedureObject and CopyFunctionObject

	}
}
