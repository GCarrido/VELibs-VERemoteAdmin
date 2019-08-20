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
		#region InvalidTableNames

		StringCollection invalidTableNames;
		StringCollection InvalidTableNames
		{
			get
			{
				if (invalidTableNames == null)
				{
					invalidTableNames = new StringCollection();
					invalidTableNames.AddRange(new string[]
					{
						"CHECK_CONSTRAINTS",
						"CONSTRAINT_TABLE_USAGE",
						"CONSTRAINT_COLUMN_USAGE",
						"COLUMNS",
						"COLUMN_PRIVILEGES",
						"COLUMN_DOMAIN_USAGE",
						"DOMAINS",
						"DOMAIN_CONSTRAINTS",
						"KEY_COLUMN_USAGE",
						"REFERENTIAL_CONSTRAINTS",
						"SCHEMATA",
						"TABLES",
						"TABLE_PRIVILEGES",
						"TABLE_CONSTRAINTS",
						"VIEWS",
						"VIEW_TABLE_USAGE",
						"VIEW_COLUMN_USAGE",
					});
				}

				return invalidTableNames;
			}
		}

		/// <summary>Verifies 'tablename' is not among standard system tables</summary>
		/// <param name="tablename"></param>
		/// <returns></returns>
		bool IsValidTablename(string tablename)
		{
			tablename = VEObjectName.Parse(tablename).ObjectName;

			return tablename.ToLower().IndexOf("sys") != 0 && !InvalidTableNames.Contains(tablename.ToUpper());
		}

		#endregion InvalidTableNames

		#region BypassTables

		StringCollection bypassTables;

		public StringCollection BypassTables
		{
			get
			{
				if (bypassTables == null)
				{
					bypassTables = new StringCollection();
					//bypassTables.Add("VEPayPeriodIDs");
					//bypassTables.AddRange(new string[]
					//{
					//"veMissingJobsEarningsHistory", "veNames", "VEPayPeriodIDs", "vePayrollSubmissions377409649", 
					//"vePayrollSubmissionsxxx", "vePERS", "vePERSSubmittalData", "veSTRS", 
					//"veSTRSChecks", "veSTRSSubmittalData", "veSubmissions", "veSubmittalData", "veSubmittalRunIDs", 
					//"veSubmittals", "veTempHistoryMisses", "veTempNames", "veTempNewEmployees", "veZeroEarningsID", 
					//"veTableSchema", "veZeroEarningsIDDetail", "Employees", "PayrollChecks", "PayrollRuns", "Submittals", 
					//"SubmittalData", "AgencyTypes", "Agencies", "CodeMapping", "EmployeeContributionHistory", "EmployeeContributions", 
					//"EmployeeEarnings", "EmployeeEarningsDetail", "EmployeeEarningsHistory", "EmployeeJobs", "EmployeePlans", 
					//"EmployeesChanges", "FiscalPeriodIDs1166159200", "FiscalPeriodIDs1635027110", "FiscalPeriods", "GLCCostCodes", 
					//"JobRates", "Jobs", "LACOEJobClassCodes", "MIPAccounts", "MIPCostAllocations", "MIPDepartments", "MIPDistribtuionApps", 
					//"MIPDistributionCodes", "MIPDistributionDetails", "MIPFiscalPeriods", "MIPFundAllocations", "MIPFundSources", 

					////"MIPSchoolTypes", "PayBasis", "PayFrequency", "PayPeriods", "PayrollRunCodes", "PayRollRunIDs1929", 
					////"PayRollRunIDs485545813", "PayScheduleCodes", "PaySchedules", "PERS_200309_3B", "PERS_200309_3M", "PERS_200309_4A", 
					////"PRRegister", "PRRegister2", "RetirementPlanCodes", "RetirementPlans", "STRS_200309_3B", "STRS_200309_3M", 
					////"STRS_200309_4A", "Submissions", "veTableDependencies", "SubmittalLocations", "Tasks", "Users", 
					////"veCheckForDuplicates", "veClientEmployeePlans", "veClientRetirees", "veComments", "veCopyContributionHistory", 
					////"veFixEarnings", "veGetEmployeeJobInfo94953", "veGetEmployeeJobInfoRS94953", "veGetJobRate", "veLACOE", "veLACOEChecks", 
					//"VELog", 
					//"VELogMessages", "veMergeContributions", "veMergeEarnings", "veMergeEarningsHistory", "veMergeEarningsSelection", 
					//"veMergeEmployeeData", "veMergeEmployeeJobs", "veMergePayroll", "veMergePayrollCheckIDs", "veMergePayrollChecks", 
					//"veMergePayrollEmployees", "veMergePayrollJobs", "veMergePayrollRuns", "veMergePayrollRunsSelection", 
					//"veMergePayrollRunsSelection2", "veMergeSelection", "veMergeSeletion", "VEMessages", "veMissingJobs", "veMissingJobsEarnings"
					//});
				}

				return bypassTables;
			}
		}

		#endregion BypassTables

		public void CopyUserObjects()
		{ 
			DateTime startTime = DateTime.Now;
			string process = "Copy Schema Objects";

			Memo("Start", string.Format("{0}: From {1}\r\n{0}:      [{2}]\r\n{0}:   To {3}\r\n{0}:      [{4}]\r\n",
										process, sourceObject.DatabaseSchema, sourceObject.Server, targetObject.DatabaseSchema, targetObject.Server));

			DropTables();

			if (!Abort)
				CopyProcedures();

			if (!Abort)
				CopyFunctions();

			if (!Abort)
				CopyTables();

			Memo("End", string.Format("\r\nCopy Schema Objects: {0, 7} {1}\r\n",
							"Elapsed", Abort ? "Operation Aborted" : VETimeStamp.ElapsedTime(startTime)));
		}

		#region CopyTables

		int copyTablesCount;
		public int CopyTablesCount { get { return copyTablesCount; } set { copyTablesCount = value; } }


		public void CopyTables()
		{
			DataTable dependencies = null;
			DateTime startTime = DateTime.Now;

			string process = "Copy Schema Tables";
			int count = 0;

			try
			{
				Memo("Start", string.Format("{0}: From {1}\r\n{0}:      [{2}]\r\n{0}:\r\n{0}:   To {3}\r\n{0}:      [{4}]\r\n", 
											process, sourceObject.DatabaseSchema, sourceObject.Server, targetObject.DatabaseSchema, targetObject.Server));

				Status("Retrieving table dependencies for " + sourceObject.DatabaseSchema);

				if((dependencies = GetTableDependencies(sourceCmd, SourceSchema)) != null)
					count = CopyTables(dependencies);
				
				else
					Memo("Error", string.Format("\r\n{0}: {1, -7} Failed to retrieve table dependencies.  Errors -\r\n{0}: {1}", process, "ERROR", SQLHandler.Errors));
			}
			//catch(Exception e)
			//{
			//	  // 
			//	  // Use this only for trouble shooting - Exceptions need to be trapped by SQLErrorHandler
			//	  // 
			//    VELog.Exception("CopyTables", e);
			//}
			finally
			{
				if (dependencies != null)
					Memo(Abort ? "Error" : "Totals", string.Format("\r\n{0}: {1, 7:n0}{2}Tables Copied Of {3}\r\n{0}: {4, -5} {5}\r\n",
										process, count,
										count == dependencies.Rows.Count ? " " : " ## ",
										dependencies.Rows.Count,
										"**",
										Abort ? "Operation Aborted" : VETimeStamp.ElapsedTime(startTime)));
			}
		}

		int CopyTables(DataTable dependencies)
		{
			Memo("Memo", "Copy Schema Tables:         Deleting table content...\r\n");

			int count = copyTablesCount = 0;

			//
			//	Delete table content in reverse order of dependency
			//

			for (int i = dependencies.Rows.Count; i-- > 0; )
			{
				targetTable.TableName = (string)dependencies.Rows[i]["Name"];
				
				string databaseOwnerTable = targetTable.DatabaseSchemaTable;
				
				if (IsValidTablename(targetTable.TableName) &&
					!BypassTables.Contains(targetTable.TableName) && targetCmd.TableExists(databaseOwnerTable))//.Name))
				{
					Status("Deleting content: " + databaseOwnerTable); 
					targetCmd.Execute("DELETE " + databaseOwnerTable); 

					if (SQLHandler.HasErrors)
					{
						Memo("Memo", "Copy Schema Tables:   ERROR Deleting " + databaseOwnerTable + "\r\n" + SQLHandler.Errors);
						copyTablesCount--;
					}
				}
			}

			if (copyTablesCount < 0)
				return 0;

			//
			//	Copy tables in ascending order of dependency
			//

			for (int i = 0; i < dependencies.Rows.Count && !Abort; i++)
			{
				sourceTable.TableName = (string)dependencies.Rows[i]["Name"];
				targetTable.TableName = sourceTable.TableName;

				if(!IsValidTablename(sourceTable.TableName) ||
					BypassTables.Contains(sourceTable.TableName))		//	BypassTables used for testing purposes
					continue;

				SQLHandler.Clear();

				do
				{
					SQLHandler.TryAgain = false;

					bool okay = CopyTable(CopySourceTable, CopyTargetTable, "Schema ");

					if (Abort || SQLHandler.HasErrors)
						break;

					if (!SQLHandler.TryAgain && okay)
					{
						copyTablesCount++;
						count++;
					}
				}
				while (SQLHandler.TryAgain); 
			}

			return count;
		}

		#endregion CopyTables

		#region CopyTable - Selected table

		public void CopyTable(string table)
		{
			targetTable.TableName = table;
			sourceTable.TableName = table;

			Memo("Memo", string.Format("Copy Selected Tables: {0,7} From [{1}].{2}", "", sourceCmd.DataControl.DataName, CopySourceTable));

			try
			{
				CopyTable(CopySourceTable, CopyTargetTable, "Selected ");
			}
			finally
			{
				Memo("Memo", "");
			}
		}

		#endregion CopyTable

		string copyModifier;

		bool CopyTable(string sourceTable, string targetTable, string modifier)
		{
			bool copyBySchema = false, okay = false;
			copyModifier = modifier;

			if(!SourceCmd.TableExists(sourceTable))
				Memo("Error", string.Format("Copy {0}Tables: {1,7} Source table not found - {2}", modifier, "Error", sourceTable));
			
			else if(!TargetCmd.TableExists(targetTable) && !(copyBySchema = CreateTargetTableSchema(sourceTable, targetTable)))
				Memo("Error", string.Format("Copy {0}Tables: {1,7} Unable to create missing target table '{2}'{3}",
									modifier, "ERROR",
									targetTable,
									!SQLHandler.HasErrors ? "" :
										string.Format(".  See errors below\r\n{0}", SQLHandler.Errors)));
			
			else if(sourceObject.ServerID == targetObject.ServerID)
			{
				int count = CopyServerTables(sourceTable, targetTable);

				if(okay = count >= 0)
					Memo("Memo", string.Format("Copy {0}Tables: {1, 7:n0} Records   {2} {3}",
												modifier, count, targetTable, copyBySchema ? "- Created by Schema" : ""));
				else
					Memo("Error", string.Format("Copy {0}Tables: {1, 7} UNABLE TO COPY records to [{2}].{3}",
						modifier, "ERROR", targetObject.ServerID, targetTable));
			}
			else
			{
				int sourceCount = 0, targetCount = 0;

				CopySourceToTargetTable(sourceTable, targetTable, ref sourceCount, ref targetCount);

				if(okay = (sourceCount == targetCount))
					Memo("Memo", string.Format("Copy {0}Tables: {1, 7:n0} Records   {2} {3}",
												modifier, sourceCount, targetTable, copyBySchema ? "- Created by Schema" : ""));
				else
				{
					Memo("Error", string.Format("Copy {0}Tables: {1, 7} {2} of {3} Records Copied To [{4}].{5} {6}{7}",
							modifier, "ERROR",
							targetCount == 0 ? "None" : "Only " + targetCount.ToString("N0"),
							sourceCount, targetObject.ServerID, targetTable, copyBySchema ? "- Created by Schema" : "",
							!SQLHandler.HasErrors ? "" : " See errors below -\r\n\r\n" + SQLHandler.Errors));
				}

			}

			return okay;
		}

		void CopySourceToTargetTable(string sourceTable, string targetTable, ref int sourceCount, ref int targetCount)
		{
			bool hasTargetTable = TargetCmd.TableExists(targetTable);

			int ix = targetTable.IndexOf('.') + 1,
				loop = 0;

			string targetUserTable = targetObject.SchemaTable, // targetTable.Substring(ix),
					txt ="";

			Abort = false;

			targetCount = 0;
			int	copySourceCount = sourceCount = SourceCmd.TableCount(sourceTable);

			Status(string.Format("Retrieving {0:n0} Records", sourceCount));

			do
			{
				if(!SourceCmd.OpenReader("SELECT * FROM " + sourceTable))
					break;

				//
				//	Replenish count if first pass generates 'IDENTITY_INSERT' errors
				//
				if (SQLHandler.TryAgain && loop++ == 0)
				{
					copySourceCount = sourceCount;
					targetCount = 0;
					SQLHandler.TryAgain = false;
				}

				try
				{
					string insertColumnNames = sourceCmd.ReaderInsertColumnNames,
							identityInsertOn = SQLHandler.Identity && sourceCmd.ReaderHasIdentity && hasTargetTable ? "SET IDENTITY_INSERT " + targetUserTable + " ON " : "",
							identityInsertOff = SQLHandler.Identity && identityInsertOn.Length > 0 ? "SET IDENTITY_INSERT " + targetUserTable + " OFF " : "";

					if(hasTargetTable)
						targetCmd.ModifyTable("DELETE " + targetUserTable);

					Status("Copying " + sourceTable + (identityInsertOn.Length > 0 ? "(*)" : "") + " - " + copySourceCount.ToString("D0"));

					StringBuilder cmd = new StringBuilder();

					while(!Abort &&
						copySourceCount > 0 &&				//	Avoids duplicate key errors
						!sourceCmd.ReaderEnd)
					{
						cmd.Length = 0; ;

						while(cmd.Length < 8000 && sourceCmd.ReaderRead())
						{
							txt = hasTargetTable
									? string.Format("{0} INSERT INTO {1} {2} VALUES {3} {4} \r\n",
												identityInsertOn,
												targetUserTable, insertColumnNames, sourceCmd.ReaderInsertColumnValues,
												identityInsertOff)
									: string.Format("SELECT {0} INTO {1}\r\n", sourceCmd.ReaderObjectValues, targetUserTable);

							copySourceCount--;

							if(cmd.Length + txt.Length < 8000)
								cmd.Append(txt);
							else
								break;

							hasTargetTable = true;		//	If there was not target table, the first 'SELECT...' will have created it!
							txt = "";

						}

						int cnt = cmd.Length > 0 ? targetCmd.ModifyTable(cmd.ToString()) : 0;

						if(cnt < 0 && txt.Length == 0)
							break;
						
						targetCount += cnt;

						if(txt.Length > 0)
							if((cnt = targetCmd.ModifyTable(txt)) < 0)
								break;
	
							else
								targetCount += cnt;

						Status(string.Format("Copying {0,8:n0} Records to {1} - {2:n0} Remaining",
									sourceCount, targetTable, copySourceCount));
					}
				}
				//catch(Exception e)
				//{
				//    VELog.Error("VEDataAdminCopy.CopySourceToTargetTable: Exception - " + e.Message);
				//}
				finally
				{
					sourceCmd.CloseReader();
					Status("");
				}
			}
			while (SQLHandler.TryAgain && !Abort);

			//if(sourceCount != targetCount)
			//    VELog.Exception("VEDataAdmin.CopySourceToTarget: ", 
			//        new Exception(string.Format("## {0} Copying Table: {1} of {2} Records Were Copied - copySourceCount = {3}; SQLTryAgain = {4}",
			//        "ERROR",
			//        targetCount == 0 ? "None" : "Only " + targetCount.ToString("N0"),
			//        sourceCount, copySourceCount, SQLTryAgain)));
		}

		#region CopyServerTables

		int copyLoop;

		int CopyServerTables(string sourceTable, string targetTable)
		{
			int copyTableCount = 0, copyTableTotal = 0, copied = 0,
					copyRecordCount = SourceCmd.TableCount(sourceTable);

			int deleted = targetCmd.ModifyTable("DELETE " + targetTable);

			for (copyLoop = 0; copyLoop < 100 && !Abort && !SQLHandler.HasErrors && copyTableCount >= 0 && (copied += copyTableCount) < copyRecordCount; )
			{
				int targetCount = targetCmd.TableCount(targetTable);

				Status(string.Format("SP Copying {0,8:n0} Records to {1} {2}",
						copyRecordCount, targetTable,
						++copyLoop > 1 ? string.Format("(Pass {0} - {1:n0} Remaining)", copyLoop, copyRecordCount - targetCount)
										: ""));

				if (copyLoop > 1)
					copied = targetCount;		//	A timeout must have occurred.  Sync up copied total to actual...

				SQLHandler.Clear();
				CopyServerTables(targetTable, ref copyTableCount, ref copyTableTotal);

				if (SQLHandler.HasErrors)
					Memo("Error", string.Format("Copy {0}Table: {1, 7} Records to {2} WERE NOT COPIED. See errors below -\r\n\r\n{3}",
								copyModifier, "ERROR", targetTable, SQLHandler.Errors));
			}

			Status("");

			return copyTableCount < 0 ? copyTableCount : copyTableTotal;
		}

		string CopyTableProcedure { get { return AdminProcedure("CopyTable"); } }

		/// <summary> Gets .copyTableParameters retrieved from AdminProcedure('CopyTable') </summary>
		VEDataParameters CopyTableParameters
		{
			get
			{
				return copyTableParameters != null ? copyTableParameters : copyTableParameters = Parameters(targetCmd, CopyTableProcedure);
			}
		}
		VEDataParameters copyTableParameters;

		/// <summary> Executes a stored procedure to copy a table from one database.schema to another database.schema on the same server.
		/// <para>Source and target objects must have been initialized using .SetSource(...) and .SetTarget(...)</para>
		/// <para>Sets 'copied' is set to the number of records copied during this iteration and</para>
		/// <para>'targetTotal' to the total count on the target table at the end of this iteration</para>
		/// <para>Note: This operation may timeout.  Call .SQLHandler() before calling this method and check .HasErrors to continue the operation. </para>
		/// </summary>
		/// <param name="table"></param>
		/// <param name="copied"></param>
		/// <param name="targetTotal"></param>
		/// <returns></returns>
		bool CopyServerTables(string table, ref int copied, ref int targetTotal)
		{
			VEObjectName veName = new VEObjectName(table);

			bool okay = SourceDatabase != TargetDatabase || SourceSchema != TargetSchema;

			if (!okay)
				message = string.Format("VEAdmin..CopyServerTables: Source and Target tables must differ - {0}.{1}.{2}", SourceDatabase, SourceSchema, veName.TableName);
			else
			{
				try
				{
					VEDataParameters parameters = CopyTableParameters;
					
					if (okay = parameters != null && parameters.Count >= 5)
					{
						parameters["@sourceDatabase"].Value = SourceDatabase;
						parameters["@sourceOwner"].Value = SourceSchema;
						parameters["@targetDatabase"].Value = TargetDatabase;
						parameters["@targetOwner"].Value = TargetSchema;
						parameters["@tableName"].Value = veName.TableName;

						if (targetCmd.ExecuteStoredProcedure(parameters) < 0)
							SQLHandler.Message = (string)parameters["@message"].Value;

						copied = (int)parameters["@copied"].Value;
						targetTotal = (int)parameters["@targetTotal"].Value;
					}
					else
					{
						message += string.Format("\r\nVEDatabaseAdmin.CopyServerTables: An error occurred retrieving parameters for procedure '{0}' on {1}", 
													CopyTableProcedure, targetCmd.DataControl.Connection);
						copied = targetTotal = -1;
					}
				}
				catch (Exception e)
				{
					//
					//	Don't report timeout exceptions
					//
					if (e.Message.ToLower().IndexOf("timeout") < 0)
					{
						Error(string.Format("VEDataAdmin.CopyServerTables: {0}Table: Trying to Copy a Table by Stored Procedure - [{1}].{2}\r\n",
										copyModifier, targetObject.ServerID, table), e);
						copyLoop = 999;
						copied = -999;
					}
				}
			}

			if (!okay)
				Error(SQLHandler.Errors);

			return okay;
		}

		#endregion CopyServerTables

		#region Create tables from database schema

		bool CreateTargetTableSchema(string sourceTable, string targetTable)
		{
			string text = GetSchemaDefinition(SourceCmd, sourceTable, targetTable);

			//VELog.Error(string.Format("## VEDataAdminCopy.CreateTargetTableSchema: GetSchemaDefinition -> \r\n{0}\r\n", text));

			if(text.Length > 0)
				TargetCmd.Execute(text);
			else
				Memo("Memo", Message);

			return TargetCmd.TableExists(targetTable);
		}

		#region GetSchemaDefinition

		/// <summary> Retrieves executable 'CREATE TABLE..' text for the specified 'sourceTable' accessible by 'sourceCmd'
		/// </summary>
		/// <param name="sourceCmd"></param>
		/// <param name="sourceTable"></param>
		/// <returns></returns>
		public string GetSchemaDefinition(VEDatabase sourceCmd, string sourceTable)
		{
			return GetSchemaDefinition(sourceCmd, sourceTable, sourceTable);
		}

		/// <summary> Retrieves executable 'CREATE TABLE..' text for specified source and target tables.  'sourceTable' is accessible by 'sourceCmd'
		/// </summary>
		/// <param name="sourceCmd"></param>
		/// <param name="sourceTable"></param>
		/// <param name="targetTable"></param>
		/// <returns></returns>
		public string GetSchemaDefinition(VEDatabase sourceCmd, string sourceTable, string targetTable)
		{
			string text = "",
					alias = Alias,
					errorMsg = "VEAdmin: Unable to generate the requisite definition for the following reason - \r\n";

			if (!ProcedureExists(sourceCmd, alias + getTableSchema))
				message = errorMsg + message;
			else
			{
				VEDataParameters parameters = Parameters(sourceCmd, alias + getTableDefinition);
				
				if (parameters == null)
					message = errorMsg + message;
				else
				{
					int s1 = sourceTable.IndexOf('.'),
						s2 = sourceTable.LastIndexOf('.'),
						t1 = targetTable.IndexOf('.'),
						t2 = targetTable.LastIndexOf('.');

					parameters["@table"].Value = s1 > 0 && s1 < s2 ? sourceTable.Substring(s2 + 1) : sourceTable.Substring(s1 + 1);
					parameters["@sourceOwner"].Value = s1 > 0 && s1 < s2 ? sourceTable.Substring(s1 + 1, s2 - s1 - 1) : sourceTable.Substring(0, s1);
					parameters["@targetOwner"].Value = t1 > 0 && t1 < t2 ? targetTable.Substring(t1 + 1, t2 - t1 - 1) : targetTable.Substring(0, t1);

					if (sourceCmd.ExecuteStoredProcedure(parameters) >= 0)
						text = (string)parameters["@tableDefinition"].Value;
				}
			}

			return text;
		}

		protected string GetSchemaDefinition(DataTable table)
		{
			string text = "CREATE TABLE " + table.TableName + " (\r\n";

			foreach (DataColumn column in table.Columns)
			{
				string dataType = column.DataType.ToString(),
					type = dataType.IndexOf(".Int") > 0 ? "INT" : "VARCHAR";

				bool isString = type == "VARCHAR";

				int width = 0;

				if (isString)
				{
					width = 256;

					foreach (DataRow row in table.Rows)
					{
						int len = row[column].ToString().Length * 2;

						if (len > width)
							width = len;
					}
				}

				text += string.Format("\t[{0}] {1}{2},\r\n", column.ColumnName, type, isString ? string.Format(" ({0})", width) : "");
			}

			return text + "\t)";
		}

		#endregion GetSchemaDefinition

		bool dboAlias = false;

		/// <summary> Get accessor performs the following:
		/// <para> 1. Checks if .AdminAccount (i.e. 'Gerald_Admin') has been set-up as a user on the database in use.  
		/// If not, the stored procedures .GetTableDependencies, .GetTableSchema and .GetTableDefinition are created for 'dbo' with all
		/// references to 'Gerald_Admin' changed to 'dbo'.</para>
		/// <para> 2. Returns .AdminAccount or 'dbo' as the owner of the .Get... procedures to be executed. </para>
		/// </summary>
		string Alias
		{
			get
			{
				string alias = (dboAlias = !sourceCmd.QueryExists("SELECT * FROM sysusers WHERE name = '" + AdminAccount + "'")) ? "dbo" : AdminAccount;

				if (dboAlias)
				{
					sourceCmd.DropProcedure(alias + getTableDependencies);
					sourceCmd.DropProcedure(alias + getTableSchema);
					sourceCmd.DropProcedure(alias + getTableDefinition);

					sourceCmd.Execute(RetrieveProcedureTextString("Gerald_Administration.Gerald_Admin" + getTableDependencies).Replace("Gerald_Admin", alias));
					sourceCmd.Execute(RetrieveProcedureTextString("Gerald_Administration.Gerald_Admin" + getTableSchema).Replace("Gerald_Admin", alias));
					sourceCmd.Execute(RetrieveProcedureTextString("Gerald_Administration.Gerald_Admin" + getTableDefinition).Replace("Gerald_Admin", alias));
				}

				return alias;
			}
		}

		#region GetTableDependencies

		/// <summary> ".GetTableDependencies" </summary>
		string getTableDependencies = ".GetTableDependencies";

		/// <summary> ".GetTableSchema" </summary>
		string getTableSchema = ".GetTableSchema";

		/// <summary> ".GetTableDefinition" </summary>
		string getTableDefinition = ".GetTableDefinition";

		/// <summary> Returns DataTable 'veTableDependencies' generated by stored procedure '[.AdminAccount].GetTableDependcies' containing schema table dependencies sequenced by increasing levels of dependency.
		/// </summary>
		/// <param name="cmd"></param>
		/// <param name="schema"></param>
		/// <returns></returns>
		public DataTable GetTableDependencies(VEDatabase cmd, string schema)
		{
			DataTable copyTableDependencies = null;

			string alias = Alias,
					procedure = alias + getTableDependencies;

			VEDataParameters parameters = Parameters(cmd, procedure);

			if (parameters == null || parameters.Count == 0)
				message = "VEDataAdmin.GetTableDependencies: Unable to create the dependencies table because of the following error -\r\n" + message + "\r\n";
			else
			{
				parameters["@owner"].Value = schema;

				if (cmd.ExecuteStoredProcedure(parameters) < 0 || parameters["@table"] == null)
					message = string.Format("VEDataAdmin.GetTableDependencies: An error occurred executing procedure '{0}'\r\n{1}", procedure, parameters.ToString());
				else
				{
					string tablename = (string)parameters["@table"].Value;

					if (cmd.Fill(tablename, string.Format("SELECT * FROM {0} ORDER BY Sequence, Name", tablename)) >= 0)
						copyTableDependencies = cmd[tablename];
					else
						message = string.Format("VEDataAdmin.GetTableDependencies: An error occurred retrieving dependencies from '{0}'", tablename);
				}
			}

			return copyTableDependencies;
		}
	
		#endregion GetTableDependencies

		#endregion Create tables from database schema

	}
}
