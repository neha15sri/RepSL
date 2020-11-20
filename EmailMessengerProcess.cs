using SwissRe.Business.Abstractions;
using SwissRe.Common.Extensions;
using SwissRe.DataAccess.Abstractions;
using SwissRe.Infrastructure.ConfigMgmt;
using SwissRe.Infrastructure.Logging;
using SwissRe.Infrastructure.SessionMgmt;
using SwissRe.Lob.Components.Messenger.Entities;
using SwissRe.Lob.Components.Messenger.Entities.Enumerations;
using SwissRe.Lob.CorSoBrCore.Business.Abstractions.BusinessFacades.QueueManager.EmailMessenger;
using SwissRe.Lob.QueueManager.Business.Abstractions.Processing;
using SwissRe.Lob.QueueManager.DataAccess.Abstractions;
using SwissRe.Lob.QueueManager.Entities;
using SwissRe.Lob.QueueManager.Entities.Extensions;
using System;
using Queue = SwissRe.Lob.QueueManager.Entities.Queue;
// ReSharper disable PossibleNullReferenceException

namespace SwissRe.Lob.CorSoBrCore.Business.BusinessFacades.QueueManager.ClaimNotifPdfCreation
{
    /// <summary>
    /// The process class that the queueRecordToProcess Windows Service would call, to process the
    /// individual tracking of messages for each receiver.
    /// </summary>
    public sealed class EmailMessengerProcess
        : BatchProcessBase
    {
        /// <summary>
        /// The unique ID of parameter of the queue item representing the message outbox number.
        /// </summary>
        private readonly int dbIdMessageNumberParam;

        private const string CFGKEY_EMAIL_MESSENGER_PARAM_01_MESSAGE_OUTBOX_ID = "keyEmailMessengerParam01_MessageOutbox_Id";

        private IConfigManager configManager;
        private IEmailMessengerBusinessFacade emailMessengerBusinessFacade;

        /// <summary>
        /// The message outbox info CRUD handler.
        /// </summary>
        private readonly IPerformCrudFor<MessageOutboxInfo> messageOutboxInfoCrud;

        #region Constructors.

        /// <summary>
        /// Instantiates this class.
        /// </summary>
        /// <param name="queueLogCrud">The queueRecordToProcess log CRUD handler.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="sessionManager">The session manager handler.</param>  
        /// <param name="queueCrud">The queueRecordToProcess CRUD handler.</param>
        /// <param name="queueTypeConfigurationComplementCrud">The queueRecordToProcess type configuration complement crud.</param>
        /// <param name="queueDataAccess">The queueRecordToProcess data access handler.</param>
        /// <param name="configManager">The configuration manager handler.</param>
        /// <param name="emailMessengerBusinessFacade">The e-mail messenger business facade handler.</param>
        /// <param name="messageOutboxInfoCrud">The message outbox info CRUD handler.</param>
        public EmailMessengerProcess
            (
                IQueueLog queueLogCrud,
                ILogger logger,
                ISessionManager sessionManager,
                ICrudFor<Queue> queueCrud,
                ICrudFor<QueueTypeConfigurationComplement> queueTypeConfigurationComplementCrud,
                IQueue queueDataAccess,
                IConfigManager configManager,
                IEmailMessengerBusinessFacade emailMessengerBusinessFacade,
                IPerformCrudFor<MessageOutboxInfo> messageOutboxInfoCrud
            )
            : base (queueDataAccess, logger, sessionManager, queueCrud, queueLogCrud, queueTypeConfigurationComplementCrud)
        {
            // Read the configuration values pertaining to the Email Messenger queue.
            this.dbIdMessageNumberParam = configManager.GetValueForParam<short>(CFGKEY_EMAIL_MESSENGER_PARAM_01_MESSAGE_OUTBOX_ID);

            // Set the instance fields witht the values specified.
            this.configManager = configManager;
            this.emailMessengerBusinessFacade = emailMessengerBusinessFacade;
            this.messageOutboxInfoCrud = messageOutboxInfoCrud;
        }

        #endregion

        #region Methods.

        /// <summary>
        /// Executes all necessary data validation and integrate Claim Notification Letter Web Services.
        /// </summary>
        /// <param name="queueRecordToProcess">Queue Item that will be processed.</param>
        /// <returns>Returns a execution status.</returns>
        public override BatchProcessStatus Execute (Queue queueRecordToProcess)
        {
            // Is the queueRecordToProcess record valid?
            if (queueRecordToProcess == null)
            {
                // No.
                base.logger.LogDebugInfo ("The queueRecordToProcess record is null. Cannot proceed with processing.");
                return (BatchProcessStatus.Error);
            }
            var processStatus = BatchProcessStatus.Success;
            var parameterValuesRead = false;
            var businessProcessingSuccessful = false;

            try
            {
                // Indicate starting of the processing.                
                var queueParameters = queueRecordToProcess.Parameters;

                // Read the queueRecordToProcess record parameter values.
                base.queueLogCrud.AddInfoLog(queueRecordToProcess, $"Initializing the processing and reading the parameter values of the queueRecordToProcess item ID {queueRecordToProcess.Id}.");

                //Get message id to be sent from Queue Parameters
                var messageIdParameter = queueRecordToProcess.Parameters.Get(this.dbIdMessageNumberParam, "MessageOutboxId");
                var messageId = int.Parse(messageIdParameter.ParameterValue);

                var messageOutboxInfoResponseBag = messageOutboxInfoCrud.GetFirst(new MessageOutboxInfo() { Id = messageId });
                if (messageOutboxInfoResponseBag.IsValid && messageOutboxInfoResponseBag.Response.IsNotNull())
                {
                    var currentMessageToBeSent = messageOutboxInfoResponseBag.Response;

                    // Prepare email message and send it.
                    var msgsSent = this.emailMessengerBusinessFacade.PrepareAndSendEmailMessage(currentMessageToBeSent);
                    if (msgsSent)
                    {
                        if (currentMessageToBeSent.IsMessageToBeTracked)
                        {
                            this.queueLogCrud.AddInfoLog(queueRecordToProcess, "Email is successfully sent via email tracker tool for message number: {0}", currentMessageToBeSent.Id);
                        }
                        else
                        {
                            this.queueLogCrud.AddInfoLog(queueRecordToProcess, "Email is successfully sent to all receivers for message number: {0}", currentMessageToBeSent.Id);
                        }

						currentMessageToBeSent.SentStatus = MessageStatus.Sent;
                        currentMessageToBeSent.SentDate = DateTime.Now;
                        currentMessageToBeSent.ModifiedDate = DateTime.Now;


					}
					else
                    {
                        if (currentMessageToBeSent.IsMessageToBeTracked)
                        {
                            this.queueLogCrud.AddInfoLog(queueRecordToProcess, "Could not schedule to send via email tracker tool  for message number: {0}", currentMessageToBeSent.Id);
                        }
                        else
                        {
                            this.queueLogCrud.AddInfoLog(queueRecordToProcess, "Could not send email to recipients for message number: {0}", currentMessageToBeSent.Id);
                        }

						currentMessageToBeSent.SentStatus = MessageStatus.Error;
					}

					messageOutboxInfoCrud.SaveExisting(currentMessageToBeSent);
				}
                else
                {
                    this.queueLogCrud.AddInfoLog(queueRecordToProcess, "The message content not found: {0}", messageId);
                }

				// Mention the completing of the processing of the queueRecordToProcess item.
				base.queueLogCrud.AddInfoLog(queueRecordToProcess, "The queueRecordToProcess item processing is complete.");
            }
            catch (Exception ex)
            {
                // Were the parameters properly read?
                if (parameterValuesRead == false)
                {
                    // No.
                    processStatus = BatchProcessStatus.InvalidParameterValue;
                }
                // Was the business processing successful?
                else if (businessProcessingSuccessful == false)
                {
                    // No.
                    processStatus = BatchProcessStatus.BusinessProcessingFailed;
                }
                else
                {
                    // The error reason is not known.
                    processStatus = BatchProcessStatus.Error;
                }

                // Log the exception.
                base.logger.LogError(String.Format("Exception occurred during the processing of the queueRecordToProcess item ID '{0}'", queueRecordToProcess.Id), ex);

                // Write to the queueRecordToProcess log table.
                this.queueLogCrud.AddErrorLog
                    (queueRecordToProcess, "Ocorreu um problema ao processar o item {0}. Detalhes: {1}", queueRecordToProcess.Id, ex.ToString());
            }
            return (processStatus);
        }

        #endregion
    }
}