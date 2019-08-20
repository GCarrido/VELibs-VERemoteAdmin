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
	/// <summary>  VESQLErrors analyzes SQL error codes by calls to the .Handler delegate.
	/// <para> If recognized, .Handler does one of two things: </para>
	/// <para> 1. Sets .TryAgain to true, indicating the error is not fatal and can be tried again - </para>
	/// <para> 1.a. Resets .Identity to indicate that a 'SET IDENTITY...' command was encountered for a table without an identity proprety </para>
	/// .............
	/// <para> 2. Stores and counts the error message for errors that should not be retried </para>
	/// <para> 2.a. Check .HasErrors to see if these errors occurred </para>
	/// <para> 2.b. Use .Errors to retrieve the summary of error messages </para>
	/// <para> 2.c. Use .Clear() to remove error conditions before executing another command </para>
	/// .............
	/// <para> .Handler returns false to indicate the error code is recognized </para>
	/// -------------
	/// <para> Note:</para>
	/// <para> . .Message can be used to add additional messages desired when reporting .Errors </para>
	/// <para> . .Text can be used to add descriptive text when reporting message counts </para>
	/// </summary>
	public class VESQLErrors
	{
		/// <summary> Instantiates a VESQLErrors object </summary>
		public VESQLErrors() { }

		/// <summary> Instantiates a VESQLErrors object using 'text' for .Errors content </summary>
		/// <param name="text"></param>
		public VESQLErrors(string text) { messages.Text = text; } 

		VEMessages messages = new VEMessages();

		/// <summary> Set: Stores and tracks the count for each unique message 'value' added </summary>
		public string Message { set { messages.Message = value; } }

		VEBooleanDelegateIntString handler;

		/// <summary> Sets the text associated with the message count to 'value' </summary>
		public string Text { set {  messages.Text = value; } }

		/// <summary> Get: Returns a delegate for analyzing SQL error codes.
		/// <para> The handler is called with 'code' and 'message' parameters. </para>
		/// <para> The handler returns false to stop error handling if 'code' is one that is recognized: </para>
		/// <para> 1. The errant statement can be re-executed.  .TryAgain can be queried to determine if the statement should be tried again </para>
		/// <para> 2. The errant statement has failed.  'message' is stored and counted for retrieval through .Errors and acknowlegement through .HasErrors </para>
		/// </summary>
		public VEBooleanDelegateIntString Handler
		{
			get
			{
				Clear();

				return handler != null ? handler : handler = new VEBooleanDelegateIntString(ErrorHandler);
			}
		}

		bool tryAgain = false, identity = false;

		/// <summary> Get: Returns true if .Handler encountered an error when 'SET IDENTIFIER ON/OFF' is attempted on a table that does not have an identity property. 
		/// <para> This indicates that the operation can be attempted again if 'SET IDENTIFIER...' is removed </para>
		/// .................
		/// <para> Set: Sets .TryAgain to value. </para>
		/// <para> It is the responsibility of the application to set .TryAgain to false so as not to re-attempt the operation unless an error occurs once again </para>
		/// -----------------
		/// <para> Note: .TryAgain is set to false when .Handler is invoked, .Clear() is called or .Handler is called to analyze an error code </para>
		/// </summary>
		public bool TryAgain { get { return tryAgain; } set { tryAgain = value; } }

		/// <summary> Get:  Returns .identity 
		/// <para> If a SQL error occurred because an attempt was made to 'SET IDENTIFIER ...' on a table that does not have an identity property, </para>
		/// <para> .Handler sets .Identity to false to indicate that 'SET IDENTIFIER ...' should be removed </para>
		/// <para> and it sets .TryAgain to true </para>
		/// </summary>
		public bool Identity { get { return identity; } }

		/// <summary> Get: Returns true if .Handler encountered errors from which recovery is not possible
		/// <para> Retrieve any error messages with .Errors </para>
		/// <para> Call .Clear() before executing a statement to remove lingering error conditions </para>
		/// </summary>
		public bool HasErrors { get { return messages.Count > 0; } }

		/// <summary> Get: Returns a list of errors encountered and the number of times each error occurred.
		/// <para> The error counts can be enhanced with text specified with .Text </para>
		/// </summary>
		public string Errors { get { return messages.ToString(); } }

		/// <summary> Clears the messages store; sets .TryAgain to false and .Identity to true </summary>
		public void Clear() 
		{ 
			tryAgain = false;
			identity = true;
			messages.Clear(); 
		}

		ArrayList tryAgainCodes = new ArrayList()
		{
			8106		//	Table '...' does not have the identity property. Cannot perform SET operation.
		};

		ArrayList sqlErrorCodes = new ArrayList()
		{
			102,		//	Incorrect syntax near the keyword '...'
			105,		//	Unclosed quotation mark after the character string '...'
			137,		//	Must declare the scalar variable "...".
			156,		//	Invalid column name ... (e.g. 'Ret Earn') for copying tables and procedures
			207,		//	Incorrect syntax near '...'
			257,		//	Implicit conversion from data type datetime to int is not allowed...
			271,		//	Column ... cannot be modified because it is a computed column
			547,		//	INSERT statement conflicted with COLUMN FOREIGN KEY constraint ...
			1750,		//	Could not create constraint. See previous errors.
			1767,		//	Foreign key 'FK...' references invalid table '...'.
			2760,		//	The specified schema name ... either does not exist or you do not have permission to use it.
			15151,		//	Cannot find the object ..., because it does not exist or you do not have permission.
		};

		/// <summary> Returns false to stop error handling if 'code' is one that is "handled":
		/// <para> 1. The errant statement can be re-executed.  .TryAgain can be queried to determine if the statement should be tried again </para>
		/// <para> 2. The errant statement has failed.  'message' is stored and counted for retrieval through .Errors and acknowlegement through .HasErrors </para>
		/// </summary>
		/// <param name="code"></param>
		/// <param name="message"></param>
		/// <returns></returns>
		bool ErrorHandler(int code, string message)
		{
			bool handled = false;

			if (!tryAgain && (tryAgain = tryAgainCodes.Contains(code)))		
				identity = !(handled = true);
			
			else
			{
				handled = sqlErrorCodes.Contains(code);

				tryAgain = false;
				identity = true;
				messages.Message = handled ? code.ToString() + " - " + message.Substring(0, message.IndexOf('\r')) : message;
			}

			return !handled;
		}
	}
}
