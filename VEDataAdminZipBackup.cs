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
		#region Backup

		#region Backup Tables

		/// <summary>
		/// <para> 1. Retrieves a list of table names from VEDatabase.RetrieveTableNames </para>
		/// <para> 2. Prepares .ZipFile for deflating table names </para>
		/// <para> 3. Has .ZipFile reserve positions in the compressed file for each table </para>
		/// <para> 4. Compresses the script for creating table 'veTableDependencies' from .backupTableDependencies. </para>
		/// <para> 5. Compresses to file .backupTableDependencies column and values strings suitable for inserting all records into to the recreated file </para>
		/// <para> 6. For each of the tables </para>
		/// <para> 6.a. Compresses to file a script suitable for recreating the table </para>
		/// <para> 6.b. Compresses to file column and values strings suitable for inserting all records into the recreated table </para>
		/// <para> 7. Communicates compression results to the calling application for each table processed </para>
		/// <para> 8. Communicates the total compression results to the calling application for all schema tables </para>
		/// </summary>
		/// <param name="schema"></param>
		/// <returns></returns>
		protected override Totals BackupTables(string schema)
		{
			DateTime startTime = DateTime.Now;
			Totals totals = null;
			int tableCount = 0, recordCount = 0, totalRecords = 0, count = 0, xcount = 0;
			string process = "Backup Schema Tables";
			
			ActiveSchema = schema;

			Memo("Memo", string.Format("//\r\n//\t{0}\r\n//\t{1}: From {2}\r\n//\t{1}:      [{3}]\r\n//\t{1}:\r\n//\t{1}:   To {4}\r\n//\t{1}:      [{5}]\r\n//\r\n",
										BackupTimeStamp, process,
										sourceObject.DatabaseSchema, sourceObject.Server, zipFile.Filename, zipFile.Path));

			string[] tablenames = sourceCmd.RetrieveTableNames(schema);

			try
			{
				Process("Backing up " + backupServerDatabase + ".");
				
				ZipFile.DeflateTableNames();

				if (tablenames.Length == 0 || !BackupTableDependencies() || backupTableDependencies.Rows.Count <= 1)
				{
					Memo("NONE", string.Format("{0}: {1, 10} There are no Tables to backup\r\n", process, "NONE"));
					xcount = tablenames.Length;
				}

				else
				{
					Abort = false;

					for (int i = 0; i < tablenames.Length && !Abort; i++)
					{
						backupObject.FileName = tablenames[i];

						if (backupObject.ObjectName.ToLower() != veTableDependencies.ToLower() &&
							backupObject.ObjectName.ToLower() != veTableSchema.ToLower() &&
							backupObject.ObjectName.ToLower() != veTableDefinition.ToLower())
							zipFile.DeflateTableName(backupObject.ObjectName);
						else
						{
							tablenames[i] = null;
							count++;
							Memo("Memo", string.Format("{0}: {1, 10} Table INTENTIONALLY NOT BACKED UP - {2}", process, "-", backupObject.SchemaTable));
						}
					}

					if (count > 0)
						Memo("Memo", "");

					count = 0;
					zipFile.DeflateTableName(null);

					if (backupTableDependencies != null)
					{
						StreamWriter stream = ZipFile.DeflateTableScript(veTableDependencies, GetSchemaDefinition(backupTableDependencies));

						BackupTableDependencies(stream, backupTableDependencies);
						totals = zipFile.DeflateTableData(veTableDependencies, stream);

						Memo("Memo", string.Format("{0}: {1,10:n0} Records, {2,10:n0} Bytes ({3,10:n0} Compressed) - {4} (Generated)",
													process, backupTableDependencies.Rows.Count - 1, totals.size, totals.deflated, veTableDependencies));

						tableCount++;
					}

					for (int i = 0; i < tablenames.Length && !Abort; i++)
					{
						xcount++;

						if (tablenames[i] != null)
						{
							backupObject.FileName = tablenames[i];

							Status(backupObject.SchemaTable);
							Application.DoEvents();

							string sql = GetSchemaDefinition(sourceCmd, backupObject.SchemaTable);
							StreamWriter backupFile = zipFile.DeflateTableScript(backupObject.ObjectName, sql);

							count = BackupTableObject(backupFile, ref recordCount);
							totals = zipFile.DeflateTableData(backupObject.ObjectName, backupFile);

							Memo("Memo", string.Format("{0}: {1, 10:n0} Records, {2,10:n0} Bytes ({3,10:n0} Compressed) - {4}",
														process, count, totals.size, totals.deflated, backupObject.TableName));

							tableCount++;
							totalRecords += count;
						}
					}

					totals = zipFile.TypeTotal;

					Memo("Memo", string.Format("\r\n{0}: {1, 10:n0} Records, {2,10:n0} Bytes ({3,10:n0} Compressed) - {4:n0} Tables Backed up",
												process, totalRecords, totals.size, totals.deflated, tableCount));
				}
			}
			catch (Exception e)
			{
				Error("VEDataAdminZip.BackupTables: " + backupServerDatabase + "." + schema, e);
				//errors.Add("VEAdminZip.BackupTables Exception: " + backupServerDatabase + "." + schema);
			}
			finally
			{
				if (xcount < tablenames.Length)
					Error(string.Format("{0}: {1, 10} Only {2:n0} of {3:n0} Tables Processed", process, "", xcount, tablenames.Length));

				else if (totals != null)
				{
					string text = string.Format("{0}: {1, 48:n1}% {2, 12} {3}\r\n",
																	process, totals.size > 0 ? 100.0 * totals.deflated / totals.size : 0.0,
																	"**", Abort ? "Operation Aborted" : VETimeStamp.ElapsedTime(startTime));

					if (!Abort)
						Memo("Memo", text);
					else
						Error(text);
				}

				ActiveSchema = null;
				Process("");
				Status("");
			}

			return totals;
		}

		public override void BackupTable(string tablename)
		{
			Memo("Error", "##\r\n##\tCompression of selected Tables is NOT IMPLEMENTED\r\n##\r\n");
		}

		protected virtual int BackupTableObject(Totals totals, ref int recordCount)
		{
			int count = 0;

			try
			{
				StreamWriter backupFile = new StreamWriter(new MemoryStream());
				count = BackupTableObject(backupFile, ref recordCount);
				zipFile.DeflateData(backupFile, null);
			}
			catch (Exception e)
			{
				MessageBox.Show(e.Message);
			}

			return count;
		}

		#endregion Backup Tables

		#region Backup Stored Objects

		protected override Totals BackupStoredObjects(string type, string schema)
		{
			DateTime startTime = DateTime.Now;
			Totals totals = null;
			string processType = "Backup Schema " + type;
			int count = 0,
				xcount = 0;

			try
			{
				ActiveSchema = schema;

				Memo(type, string.Format("//\r\n//\t{0}\r\n//\t{1}: From {2}\r\n//\t{1}:      [{3}]\r\n//\t{1}:\r\n//\t{1}:   To {4}\r\n//\t{1}:      [{5}]\r\n//\r\n",
										BackupTimeStamp, processType,
										sourceObject.DatabaseSchema, sourceObject.Server, zipFile.Filename, zipFile.Path));

				Process("Backing up " + backupServerDatabase + ".");
				Status(string.Format("{0} to {1}", type, zipFile.FileDatabaseSchema));

				Abort = false;
				string[] objectNames = type == "Procedures" ? sourceCmd.RetrieveUserProcedures(schema)
															: sourceCmd.RetrieveFunctionNames(schema);

				if (objectNames.Length > 0)
				{
					foreach (string name in objectNames)
						if (name.IndexOf(schema + ".") == 0)
							zipFile.DeflateObjectName(schema, type, name);

					if ((count = zipFile.DeflateObjectName(null)) > 0)
						foreach (string name in objectNames)
							if (name.IndexOf(schema + ".") == 0)
							{
							Status(name);
							totals = zipFile.DeflateStoredObject(name, sourceCmd.RetrieveProcedureText(name));
							xcount++;

							Memo(type, string.Format("{0}: {1, 10:n0} Bytes ({2,10:n0} Compressed) {3}", processType, totals.size, totals.deflated, name));
							Status("");

							if (Abort)
								break;
						}
					
				}

				if (count == 0 || (totals = zipFile.TypeTotal).count == 0)
					Memo("NONE", string.Format("{0}: {1, -10} There are no Schema-qualified {0} to backup\r\n", processType, "NONE"));
				
				else if (totals != null)
					Memo("Memo", string.Format("\r\n{0}: {1, 10:n0} Bytes ({2,10:n0} Compressed) - {3:n0} {0} Copied ", processType, totals.size, totals.deflated, totals.count));
			}
			catch (Exception e)
			{
				Error("VEDataAdminZip.BackupStoredObjects: " + backupServerDatabase + "." + schema + "." + type, e);
				//errors.Add("VEAdmin.BackupStoredObjects Exception: " + backupServerDatabase + "." + schema + "." + type);
			}
			finally
			{
				if (xcount < count)
					Error(string.Format("{0}: {1, 10} Only {2:n0} of {3:n0} {0} Copied", processType, "", xcount, count));
				
				else if (totals != null && totals.count > 0)
					Memo(Abort ? "Error" : "Memo", string.Format("{0}: {1, 28:n1}% {2, 12} {3}\r\n",
												processType, totals.size > 0 ? 100.0 * totals.deflated / totals.size : 0.0,
												"**", Abort ? "Operation Aborted" : VETimeStamp.ElapsedTime(startTime)));

				Process("");
				Status("");

				ActiveSchema = null;
			}

			return totals;
		}

		public override void BackupProcedure(string name)
		{
			Memo("Error", "##\r\n##\tCompression of selected Procedures is NOT IMPLEMENTED\r\n##\r\n");
		}

		public override void BackupFunction(string name)
		{
			Memo("Error", "##\r\n##\tCompression of selected Functions is NOT IMPLEMENTED\r\n##\r\n");
		}

		#endregion Backup Stored Objects

		#endregion Backup

	}
}
