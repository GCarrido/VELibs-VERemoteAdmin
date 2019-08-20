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
	/// <summary> Derives from VEHandlers
	/// <para> =============== </para>
	/// <para> VESQLHandlers stacks delegate handlers for SQL errors. It initializes the stack with an internal VESQLErrors object handler </para>
	/// <para> Handlers must have a (int errorCode, string errorMessage) signature </para>
	/// <para> . Call Add/Remove(VEBooleanDelegateIntString) to add/Remove VEBooleanDelegateIntString handlers </para>
	/// <para> . Call Add/Remove(VESQLErrors errors) to add/Remove errors.Handler </para>
	/// <para> . Call Handle(code, message) to handle an error condition </para>
	/// <para> . Invoke .Handler for the default VESQLErrors object handler </para>
	/// ------------------
	/// <para> .Handle(..) calls the delegate handlers in reverse order </para>
	/// <para> .Handle(..) returns true if the error code was handled by one of the delegate handlers , false otherwise </para>
	/// ------------------
	/// <para> See VESQLErrors for handling error conditions recognized through development work </para>
	/// <para> The following methods and properties pass through those for the internal VESQLErrors object </para>
	/// <para> . .HasErrors - indicates whether or not errors were encounterd </para>
	/// <para> . .Errors - the errors encountered and occurrence counts </para>
	/// <para> . .TryAgain - indicates whether or not an encountered error can be corrected dynamically </para>
	/// <para> . .Identity - indicates whehter or not the 'INSERT IDENTITY ..' modifier needs to be removed as a dynamic correction </para>
	/// <para> . .Clear() - to clear messages and reset indicators </para>
	/// <para> . .Message- to add process-specific messages to the message store </para>
	/// <para> . .Text - to provide a descriptor of error counts other than 'Occurrences' </para>
	/// </summary>
	public class VESQLHandlers : VEHandlers
	{
		static VESQLErrors sqlHandler = new VESQLErrors("Occurrence");

		/// <summary> Instantiates a VESQLHandlers object adding an internal static VESQLErrors handler to the handlers stack  </summary>
		public VESQLHandlers() : base(sqlHandler.Handler) { }

		/// <summary> Instantiates a VESQLHandlers object adding 'handler' to the handlers stack </summary>
		/// <param name="handler"></param>
		public VESQLHandlers(VEBooleanDelegateIntString handler) : base(handler) { }

		/// <summary> Get: Returns the static, internal VESQLErrors object whose .Handler was used to initialize the handlers stack </summary>
		public VESQLErrors Handler { get { return sqlHandler; } }

		public new VEBooleanDelegateIntString First { get { return (VEBooleanDelegateIntString)base.First; } }
		public new VEBooleanDelegateIntString Next { get { return (VEBooleanDelegateIntString)base.Next; } }
		public new VEBooleanDelegateIntString Previous { get { return (VEBooleanDelegateIntString)base.Previous; } }
		public new VEBooleanDelegateIntString Last { get { return (VEBooleanDelegateIntString)base.Last; } }

		/// <summary> Adds sqlErrors.Handler to the handlers stack </summary>
		/// <param name="sqlErrors"></param>
		public void Add(VESQLErrors sqlErrors)
		{
			Add(sqlErrors.Handler);
		}

		/// <summary> Removes sqlErrors.Handler from the stack as long 'sqlErrors' is not the internal VESQLErrors .Handler object </summary>
		/// <param name="sqlErrors"></param>
		public void Remove(VESQLErrors sqlErrors)
		{
			if (sqlErrors != sqlHandler)
				Remove(sqlErrors.Handler);
		}

		/// <summary> Removes 'handler' from the stack as long it is not the internal handler </summary>
		/// <param name="handler"></param>
		public void Remove(VEBooleanDelegateIntString handler)
		{
			if (handler != sqlHandler.Handler)
				base.Remove(handler);
		}

		/// <summary> Passes 'code' and 'message' to each stacked handler delegate.
		/// <para> Returns true when the first of them handles the error, false if none did </para>
		/// </summary>
		/// <param name="code"></param>
		/// <param name="message"></param>
		/// <returns></returns>
		public bool Handle(int code, string message)
		{
			bool handled = false;

			for (VEBooleanDelegateIntString handler = Last; handler != null; handler = Previous)
				if (handled = !handler(code, message))
					break;

			return handled;
		}

		#region VESQLErrors object passthrus

		/// <summary> Clears the internal VESQLErrors object's messages store; sets .TryAgain to false and .Identity to true </summary>
		public void Clear() { sqlHandler.Clear(); }

		/// <summary> Get: Returns true if the internal VESQLErrors object encountered an error when 'SET IDENTIFIER ON/OFF' is attempted on a table that does not have an identity property. 
		/// <para> This indicates that the operation can be attempted again if 'SET IDENTIFIER...' is removed </para>
		/// .................
		/// <para> Set: Sets .TryAgain to value. </para>
		/// <para> It is the responsibility of the application to clear .TryAgain so as not to re-attempt the operation unless an error occurs once again </para>
		/// -----------------
		/// <para> Note: .TryAgain is cleared when .Clear() is called </para>
		/// </summary>
		public bool TryAgain { get { return sqlHandler.TryAgain; } set { sqlHandler.TryAgain = value; } }

		/// <summary> Get:  Returns the internal VESQLErrors object's .identity property
		/// <para> If a SQL error occurred because an attempt was made to 'SET IDENTIFIER ...' on a table that does not have an identity property, </para>
		/// <para> the internal VESQLErrors object clears .Identity to indicate that 'SET IDENTIFIER ...' should be removed </para>
		/// <para> and it sets .TryAgain to true </para>
		/// </summary>
		public bool Identity { get { return sqlHandler.Identity; } }

		/// <summary> Get: Returns true if the internal VESQLErrors object encountered errors from which recovery is not possible
		/// <para> Retrieve any error messages with .Errors </para>
		/// <para> Call .Clear() before executing a statement to remove lingering error conditions </para>
		/// </summary>
		public bool HasErrors { get { return sqlHandler.HasErrors; } }

		/// <summary> Get: Returns a list of errors the internal VESQLErrors object encountered and the number of times each error occurred.
		/// </summary>
		public string Errors { get { return sqlHandler.Errors; } }

		/// <summary> Set: Stores and tracks the count for each unique message 'value' </summary>
		public string Message { set { sqlHandler.Message = value; } }
	
		#endregion VESQLErrors object passthrus

	}

}
