using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using System.Collections.Generic;
using Flurl.Http;
using Newtonsoft.Json;
using x.Common.Extensions;
using x.Common.Logging;
using x.Common.Messaging;
using x.Domain;
using x.Domain.Services.Payments;
using x.Infrastructure.Services.Tokenization.TokenExTransparent;
using System.Xml.Serialization;
using System.IO;
using System.Diagnostics;
using x.Domain.Services.Orders;
using System.Linq;
using System.Text;

namespace x.Infrastructure.Services.Payments.Processor
{
	public class ProcessorPaymentGatewayService : IPaymentGatewayService
	{
		private readonly ProcessorGatewayConfig _config;
		private readonly TokenExTokenizerConfig _tokenizerConfig;
		private readonly TokenExTransparentGatewayConfig _tokenExTransparentGatewayConfig;
		private readonly TokenExTransparentGatewayService _tokenExTransparentGatewayService;
		private readonly IOrderStatusUpdater _orderStatusUpdater;

		readonly PaymentGatewayProfile _paymentGatewayProfile;
		readonly ProcessorNetConnectPaymentGatewaySettings _profileSettings;
		readonly IPaymentGatewayTransactionRepository _paymentGatewayTransactionRepository;

		public ProcessorPaymentGatewayService(PaymentGatewayProfile paymentGatewayProfile,
			IPaymentGatewayTransactionRepository paymentGatewayTransactionRepository,
			ProcessorGatewayConfig config, TokenExTokenizerConfig tokenizerConfig,
			TokenExTransparentGatewayConfig tokenExTransparentGatewayConfig,
			IOrderStatusUpdater orderStatusUpdater)
		{
			_paymentGatewayProfile = paymentGatewayProfile;
			_paymentGatewayTransactionRepository = paymentGatewayTransactionRepository;
			_profileSettings = JsonConvert.DeserializeObject<ProcessorNetConnectPaymentGatewaySettings>(paymentGatewayProfile.ProfileSettings);
			_config = config;
			_orderStatusUpdater = orderStatusUpdater;
			_tokenizerConfig = tokenizerConfig;
			_tokenExTransparentGatewayConfig = tokenExTransparentGatewayConfig;
			_tokenExTransparentGatewayService = new TokenExTransparentGatewayService(_tokenExTransparentGatewayConfig)
			{
				Headers = new Dictionary<string, string>
				{
					{"MIME-Version", "application/xml"},
					{"Content-transfer-encoding", "application/xml"},
					{"Request-number", "1"},
					{"Document-type", "Request"},
					{"Content-Type", "application/[hidden to protect the innocent]"},
					{"TX_URL", _config.Url}
				}
			};
		}

		public async Task<AuthorizePaymentResponse> AuthorizePaymentAsync(PaymentRequest paymentRequest)
		{
			PaymentTransactionType paymentTransactionType = PaymentTransactionType.Authorize;
			CreditCard creditCard = await GetTokenizedCreditCard(paymentRequest);
			ProcessorPaymentRequest ProcessorPaymentRequest = CreateRequest(paymentRequest, creditCard, TransactionType.Auth);
			ProcessorPaymentRequest.NewOrder.AccountNum = string.Concat(@"{{{", creditCard.CardNumber, @"}}}"); // Transparent Gateway format (tokenized CC)

			Log Log = null;
			string amount = paymentRequest.Amount.ToString();
			string responseString;
			string logMsg;
			string orderId = paymentRequest.ReferenceData.GetByKeyOrDefault("OrderId") ?? "";

			try
			{
				XmlSerializer serializer = new XmlSerializer(ProcessorPaymentRequest.GetType());
				using (StringWriter sw = new StringWriter())
				{
					serializer.Serialize(sw, ProcessorPaymentRequest);
					var ProcessorXmlFormatting = ProcessorXmlFormatting(sw.ToString());

					using (StringReader sr = new StringReader(await _tokenExTransparentGatewayService.DetokenizeAsync(ProcessorXmlFormatting)))
					{
						responseString = sr.ReadToEnd();
						Log = new Log()
						{
							Amount = amount,
							CreditCard = creditCard,
							OrderId = orderId,
							PaymentTransactionType = paymentTransactionType,
							ResponseString = responseString,
							RequestString = ProcessorXmlFormatting.Replace("\r\n", "")
						};
						logMsg = Log.LogTransaction();
						if (!string.IsNullOrEmpty(logMsg)) 
						{
							throw new Exception(logMsg);
						}
					}

					return GetAuthPaymentResponse
					(
						Log.ProcessorPaymentResponse,
						paymentRequest,
						creditCard
					);
				}
			}
			catch (FlurlHttpException ex)
			{
				logMsg = string.Format("Processor Auth: {0}{1}{2}{3}{4}", ex.Message, ". Order #",
					paymentRequest.ReferenceData.GetByKeyOrDefault("OrderId") ?? "", ", Card Number: ", creditCard.MaskedCardNumber);
				Logger.Current.Write(logMsg, TraceEventType.Error);
				throw new Exception(ex.Message);
			}
			catch (Exception ex)
			{
				Logger.Current.Write(ex.Message, TraceEventType.Error);
				throw new Exception(ex.Message);
			}
		}

		public async Task<PaymentResponse> MakePaymentAsync(PaymentRequest paymentRequest)
		{
			PaymentTransactionType paymentTransactionType = PaymentTransactionType.Payment;
			CreditCard creditCard = await GetTokenizedCreditCard(paymentRequest);
			ProcessorPaymentRequest ProcessorPaymentRequest = CreateRequest(paymentRequest, creditCard, TransactionType.AuthCapture);
			ProcessorPaymentRequest.NewOrder.AccountNum = string.Concat(@"{{{", creditCard.CardNumber, @"}}}"); // Transparent Gateway format (tokenized CC)
			string amount = paymentRequest.Amount.ToString();
			Log Log = null;
			string responseString;
			string logMsg;

			try
			{
				XmlSerializer serializer = new XmlSerializer(ProcessorPaymentRequest.GetType());
				using (StringWriter sw = new StringWriter())
				{
					serializer.Serialize(sw, ProcessorPaymentRequest);
					var ProcessorXmlFormatting = ProcessorXmlFormatting(sw.ToString());

					using (StringReader sr = new StringReader(await _tokenExTransparentGatewayService.DetokenizeAsync(ProcessorXmlFormatting)))
					{
						responseString = sr.ReadToEnd();
						Log = new Log()
						{
							Amount = amount,
							CreditCard = creditCard,
							//OrderId = orderId,
							PaymentTransactionType = paymentTransactionType,
							ResponseString = responseString,
							RequestString = ProcessorXmlFormatting.Replace("\r\n", "")
						};
						logMsg = Log.LogTransaction();
						if (!string.IsNullOrEmpty(logMsg))
						{
							throw new Exception(logMsg);
						}
					}
					return GetPaymentResponse
					(
						Log.ProcessorPaymentResponse,
						paymentRequest,
						creditCard
					);
				}
			}
			catch (FlurlHttpException ex)
			{
				logMsg = string.Format("Processor MakePayment: {0}{1}{2}{3}{4}", ex.Message, ". Order #",
					paymentRequest.ReferenceData.GetByKeyOrDefault("OrderId") ?? "", ", Card Number: ", creditCard.MaskedCardNumber);
				Logger.Current.Write(logMsg, TraceEventType.Error);
				throw new Exception(ex.Message);
			}
			catch (Exception ex)
			{
				Logger.Current.Write(ex.Message, TraceEventType.Error);
				throw new Exception(ex.Message);
			}
		}

		public async Task<PaymentResponse> CapturePaymentAsync(CapturePaymentRequest capturePaymentRequest)
		{
			PaymentTransactionType paymentTransactionType = PaymentTransactionType.Capture;
			CreditCard creditCard = capturePaymentRequest.PaymentAccessor.GetCreditCard();
			ProcessorCaptureRequest ProcessorCaptureRequest = CreateRequest(capturePaymentRequest);
			Log Log;
			string responseString;
			string logMsg;
			string amount = capturePaymentRequest.AuthorizedAmount.ToString();
			string orderId = "";

			if (capturePaymentRequest.ReferenceData != null && capturePaymentRequest.ReferenceData.ContainsKey("OrderId"))
			{
				orderId = capturePaymentRequest.ReferenceData["OrderId"];
			}

			try
			{
				XmlSerializer serializer = new XmlSerializer(ProcessorCaptureRequest.GetType());
				using (StringWriter sw = new StringWriter())
				{
					serializer.Serialize(sw, ProcessorCaptureRequest);
					var ProcessorXmlFormatting = ProcessorXmlFormatting(sw.ToString());

					responseString = await FlurlRequest(ProcessorXmlFormatting, _config.Url);
					Log = new Log()
					{
						Amount = amount,
						CreditCard = creditCard,
						OrderId = orderId,
						PaymentTransactionType = paymentTransactionType,
						ResponseString = responseString,
						RequestString = ProcessorXmlFormatting.Replace("\r\n", "")
					};
					logMsg = Log.LogTransaction();
					if (!string.IsNullOrEmpty(logMsg))
					{
						throw new Exception(logMsg);
					}

					return GetCapturePaymentResponse
					(
						Log.ProcessorCaptureResponse,
						capturePaymentRequest,
						creditCard
					);
				}
			}
			catch (FlurlHttpException ex)
			{
				logMsg = string.Format("Processor Capture: {0}{1}{2}{3}{4}", ex.Message, ". Order #", orderId, ", Card Number: ", creditCard.MaskedCardNumber);
				Logger.Current.Write(logMsg, TraceEventType.Error);
				throw new Exception(ex.Message);
			}
			catch (Exception ex)
			{
				Logger.Current.Write(ex.Message, TraceEventType.Error);
				throw new Exception(ex.Message);
			}
		}

		public async Task<VoidPaymentResponse> VoidPaymentAsync(VoidPaymentRequest voidPaymentRequest)
		{
			PaymentTransactionType paymentTransactionType = PaymentTransactionType.Void;
			string orderId = "";
			Log Log;
			string responseString;
			string logMsg;

			if (voidPaymentRequest.ReferenceData != null && voidPaymentRequest.ReferenceData.ContainsKey("OrderId"))
			{
				orderId = voidPaymentRequest.ReferenceData["OrderId"];
				await _orderStatusUpdater.CancelOrder(orderId.ToInt32());
			}

			ProcessorReversalRequest ProcessorReversalRequest = CreateRequest(voidPaymentRequest);
			XmlSerializer serializer = new XmlSerializer(ProcessorReversalRequest.GetType());

			try
			{
				using (StringWriter sw = new StringWriter())
				{
					serializer.Serialize(sw, ProcessorReversalRequest);
					var ProcessorXmlFormatting = ProcessorXmlFormatting(sw.ToString());

					responseString = await FlurlRequest(ProcessorXmlFormatting, _config.Url);
					Log = new Log()
					{
						OrderId = orderId,
						PaymentTransactionType = paymentTransactionType,
						ResponseString = responseString,
						VoidPaymentRequest = voidPaymentRequest,
						RequestString = ProcessorXmlFormatting.Replace("\r\n", "")
					};
					logMsg = Log.LogTransaction();
					if (!string.IsNullOrEmpty(logMsg))
					{
						throw new Exception(logMsg);
					}

					return new VoidPaymentResponse
					{
						AuthorizedAmount = voidPaymentRequest.AuthorizedAmount,
						PaymentGatewayProfile = _paymentGatewayProfile,
						PaymentGatewayTransactionId = RecordTransaction
						(
							Log.ProcessorVoidResponse,
							ProcessorReversalRequest,
							paymentTransactionType,
							voidPaymentRequest.CreditCard
						)
					};
				}
			}
			catch (FlurlHttpException ex)
			{
				logMsg = string.Format("Processor Void: {0}{1}{2}", ex.Message, ". Order #:", orderId);
				Logger.Current.Write(logMsg, TraceEventType.Error);
				throw new Exception(ex.Message);
			}
			catch (Exception ex)
			{
				Logger.Current.Write(ex.Message, TraceEventType.Error);
				throw new Exception(ex.Message);
			}
		}

		public async Task<RefundResponse> RefundPaymentAsync(RefundRequest refundRequest)
		{
			PaymentTransactionType paymentTransactionType = PaymentTransactionType.Refund;
			Log Log;
			string responseString;
			string logMsg;
			string orderId = refundRequest.OrderId > 0 ? refundRequest.OrderId.ToString() : refundRequest.ReferenceData.GetByKeyOrDefault("OrderId");
			string amount = refundRequest.Amount.ToString();

			ProcessorPaymentRequest ProcessorPaymentRequest = CreateRequest(refundRequest, TransactionType.Refund);
			XmlSerializer serializer = new XmlSerializer(ProcessorPaymentRequest.GetType());
			using (StringWriter sw = new StringWriter())
			{
				serializer.Serialize(sw, ProcessorPaymentRequest);
				var ProcessorXmlFormatting = ProcessorXmlFormatting(sw.ToString());

				try
				{
					responseString = await FlurlRequest(ProcessorXmlFormatting, _config.Url);
					Log = new Log()
					{
						Amount = amount,
						OrderId = orderId,
						PaymentTransactionType = paymentTransactionType,
						ResponseString = responseString,
						RequestString = ProcessorXmlFormatting.Replace("\r\n", "")
					};
					logMsg = Log.LogTransaction();
					if (!string.IsNullOrEmpty(logMsg))
					{
						throw new Exception(logMsg);
					}

					return new RefundResponse
					{
						TransactionId = Log.ProcessorPaymentResponse.NewOrderResp.RefNum,
						PaymentGatewayProfile = _paymentGatewayProfile,
						PaymentGatewayTransactionId = RecordTransaction(Log.ProcessorPaymentResponse, refundRequest.Amount, PaymentTransactionType.Refund, new CreditCardPartial(refundRequest.CreditCard))
					};
				}
				catch (FlurlHttpException ex)
				{
					logMsg = string.Format("Processor Refund: {0}{1}{2}", ex.Message, ". Order #:", orderId);
					Logger.Current.Write(logMsg, TraceEventType.Error);
					throw new Exception(ex.Message);
				}
				catch (Exception ex)
				{
					Logger.Current.Write(ex.Message, TraceEventType.Error);
					throw new Exception(ex.Message);
				}
			}
		}

		// Build Request for Void
		private ProcessorReversalRequest CreateRequest(VoidPaymentRequest voidPaymentRequest)
		{
			string orderNumber = voidPaymentRequest.ReferenceData.GetByKeyOrDefault("OrderId") ?? "";
			return new ProcessorReversalRequest()
			{
				Reversal = new Reversal()
				{
					AdjustedAmt = String.Format("{0:0.00}", voidPaymentRequest.AuthorizedAmount).Replace(".", ""),
					BIN = "000001",
					MerchantID = _profileSettings.ProcessorMerchantId.ToInt32(),
					OnlineReversalInd = "N",
					ConnectionPassword = _profileSettings.Password,
					ConnectionUsername = _profileSettings.UserName,
					OrderID = orderNumber,
					TerminalID = _profileSettings.TerminalId,
					RefNum = voidPaymentRequest.TransactionId
				}
			};
		}

		// Build Request for Refund
		private ProcessorPaymentRequest CreateRequest(RefundRequest refundRequest, TransactionType transactionType)
		{
			string orderNumber = refundRequest.OrderId > 0 ? refundRequest.OrderId.ToString() : refundRequest.ReferenceData.GetByKeyOrDefault("OrderId");

			return new ProcessorPaymentRequest()
			{
				NewOrder = new NewOrder()
				{
					Amount = String.Format("{0:0.00}", -refundRequest.Amount).Replace(".", ""),
					BIN = "000001",
					CurrencyCode = "[Hidden to Protect the Innocent]",
					CurrencyExponent = "[Hidden to Protect the Innocent]",
					IndustryType = "[Hidden to Protect the Innocent]",
					MerchantID = _profileSettings.ProcessorMerchantId.ToInt32(),
					MessageType = ToEnumString(transactionType),
					ConnectionPassword = _profileSettings.Password,
					ConnectionUsername = _profileSettings.UserName,
					OrderID = orderNumber,
					TerminalID = _profileSettings.TerminalId,
					RefNum = refundRequest.TransactionId
				}
			};
		}

		// Build Request for Auth Only and Make Payment
		private ProcessorPaymentRequest CreateRequest(PaymentRequest paymentRequest, CreditCard creditCard, TransactionType transactionType)
		{
			string expirationMM = creditCard.ExpirationMonth.ToString("D2");
			string expirationYY = creditCard.ExpirationYear.ToString().Length == 4 ? creditCard.ExpirationYear.ToString().Substring(2, 2) : creditCard.ExpirationYear.ToString();
			string address1 = Coalesce(creditCard.AddressLine1, paymentRequest.CardHolder?.Address?.AddressLine1);
			string city = Coalesce(creditCard.City, paymentRequest.CardHolder?.Address?.City);

			city = city.Length > 19 ? city.Substring(0, 19) : city;
			address1 = address1.Length > 29 ? address1.Substring(0, 29) : address1;

			ProcessorPaymentRequest ProcessorPaymentRequest = new ProcessorPaymentRequest()
			{
				NewOrder = new NewOrder()
				{
					AccountNum = creditCard.CardNumber,
					Amount = String.Format("{0:0.00}", paymentRequest.Amount).Replace(".", ""),
					AVSaddress1 = address1,
					AVSaddress2 = "",
					AVScity = city,
					AVSphoneNum = "",
					AVSstate = Coalesce(creditCard.State, paymentRequest.CardHolder?.Address?.StateAbbreviation),
					AVSzip = Coalesce(creditCard.PostalCode, paymentRequest.CardHolder?.Address?.Zipcode),
					BIN = "000001",
					CardSecVal = creditCard.CardVerificationValue,
					CardSecValInd = !string.IsNullOrEmpty(creditCard.CardVerificationValue) ? 1 : 0,
					CurrencyCode = "840",
					CurrencyExponent = 2,
					Exp = string.Concat(expirationMM, expirationYY),
					IndustryType = "EC",
					MerchantID = _profileSettings.ProcessorMerchantId.ToInt32(),
					MessageType = ToEnumString(transactionType),
					ConnectionPassword = _profileSettings.Password,
					ConnectionUsername = _profileSettings.UserName,
					OrderID = paymentRequest.ReferenceData.GetByKeyOrDefault("OrderId") ?? "",
					SDMerchantName = String.Concat(Encoding.ASCII.GetString(Encoding.GetEncoding("Cyrillic")
						.GetBytes(paymentRequest.ReferenceData["LocationName"])).Where(ch => Char.IsLetterOrDigit(ch))).Left(11),
					TerminalID = _profileSettings.TerminalId
				}
			};
			return ProcessorPaymentRequest;
		}

		// Build Request for Capture
		private ProcessorCaptureRequest CreateRequest(CapturePaymentRequest capturePaymentRequest)
		{
			string orderNumber = "";
			if (capturePaymentRequest.ReferenceData != null && capturePaymentRequest.ReferenceData.ContainsKey("OrderId"))
			{
				orderNumber = capturePaymentRequest.ReferenceData["OrderId"];
			}

			return new ProcessorCaptureRequest()
			{
				MarkForCapture = new MarkForCapture()
				{
					Amount = String.Format("{0:0.00}", capturePaymentRequest.AuthorizedAmount).Replace(".", ""),
					BIN = "000001",
					MerchantID = _profileSettings.ProcessorMerchantId.ToInt32(),
					ConnectionPassword = _profileSettings.Password,
					ConnectionUsername = _profileSettings.UserName,
					OrderID = orderNumber,
					TerminalID = _profileSettings.TerminalId,
					RefNum = capturePaymentRequest.TransactionId
				}
			};
		}

		private PaymentResponse GetPaymentResponse(ProcessorPaymentResponse ProcessorPaymentResponse, PaymentRequest paymentRequest, CreditCard creditCard)
		{
			var paymentGatewayTransactionId = PublishAndRecord(ProcessorPaymentResponse, paymentRequest, creditCard, PaymentTransactionType.Payment);
			return new PaymentResponse
			{
				PaymentAmount = paymentRequest.Amount,
				AuthorizationCode = ProcessorPaymentResponse.NewOrderResp.AuthCode,
				TransactionId = ProcessorPaymentResponse.NewOrderResp.RefNum,
				PaymentGatewayProfile = _paymentGatewayProfile,
				CreditCard = new CreditCardPartial(creditCard),
				PaymentGatewayTransactionId = paymentGatewayTransactionId,
			};
		}

		private AuthorizePaymentResponse GetAuthPaymentResponse(ProcessorPaymentResponse ProcessorPaymentResponse, PaymentRequest paymentRequest, CreditCard creditCard)
		{
			var paymentGatewayTransactionId = PublishAndRecord(ProcessorPaymentResponse, paymentRequest, creditCard, PaymentTransactionType.Authorize);
			return new AuthorizePaymentResponse
			{
				PaymentRequest = paymentRequest,
				AuthorizedAmount = paymentRequest.Amount,
				AuthorizationCode = ProcessorPaymentResponse.NewOrderResp.AuthCode,
				TransactionId = ProcessorPaymentResponse.NewOrderResp.RefNum,
				PaymentGatewayProfile = _paymentGatewayProfile,
				CreditCard = new CreditCardPartial(creditCard),
				PaymentGatewayTransactionId = paymentGatewayTransactionId
			};
		}

		private PaymentResponse GetCapturePaymentResponse(ProcessorCaptureResponse ProcessorCaptureResponse, CapturePaymentRequest paymentRequest, CreditCard creditCard)
		{
			var paymentGatewayTransactionId = PublishAndRecord(ProcessorCaptureResponse, paymentRequest, creditCard, PaymentTransactionType.Capture);
			return new PaymentResponse
			{
				PaymentAmount = decimal.Parse(ProcessorCaptureResponse.MarkForCaptureResp.Amount) / 100,
				AuthorizationCode = ProcessorCaptureResponse.MarkForCaptureResp.AuthCode,
				TransactionId = ProcessorCaptureResponse.MarkForCaptureResp.RefNum,
				PaymentGatewayProfile = _paymentGatewayProfile,
				CreditCard = new CreditCardPartial(creditCard),
				PaymentGatewayTransactionId = paymentGatewayTransactionId
			};
		}

		private Guid PublishAndRecord(ProcessorPaymentResponse ProcessorPaymentResponse, PaymentRequest paymentRequest, CreditCard creditCard, PaymentTransactionType paymentTransactionType)
		{
			EventPublisher.Current.Publish(new PaymentEvent
			{
				Amount = paymentRequest.Amount,
				PaymentDescription = paymentRequest.PaymentDescription,
				AuthorizationCode = ProcessorPaymentResponse.NewOrderResp.AuthCode,
				TransactionId = ProcessorPaymentResponse.NewOrderResp.RefNum,
				PaymentGatewayProfile = _paymentGatewayProfile,
				TransactionType = paymentTransactionType
			});
			return RecordTransaction(ProcessorPaymentResponse, paymentRequest.Amount, paymentTransactionType, new CreditCardPartial(creditCard));
		}

		private Guid PublishAndRecord(ProcessorCaptureResponse ProcessorCaptureResponse, CapturePaymentRequest paymentRequest, CreditCard creditCard, PaymentTransactionType paymentTransactionType)
		{
			EventPublisher.Current.Publish(new PaymentEvent
			{
				Amount = decimal.Parse(ProcessorCaptureResponse.MarkForCaptureResp.Amount) / 100,
				PaymentDescription = paymentRequest.PaymentDescription,
				AuthorizationCode = ProcessorCaptureResponse.MarkForCaptureResp.AuthCode,
				TransactionId = ProcessorCaptureResponse.MarkForCaptureResp.RefNum,
				PaymentGatewayProfile = _paymentGatewayProfile,
				TransactionType = paymentTransactionType
			});
			return RecordTransaction(ProcessorCaptureResponse, paymentTransactionType, new CreditCardPartial(creditCard));
		}

		private Guid RecordTransaction(ProcessorVoidResponse ProcessorVoidResponse, ProcessorReversalRequest ProcessorReversalRequest, PaymentTransactionType paymentTransactionType, CreditCardPartial creditCard)
		{
			var entry = new PaymentGatewayTransaction
			{
				PaymentGatewayProfileId = _paymentGatewayProfile.ProfileId,
				Amount = decimal.Parse(ProcessorReversalRequest.Reversal.AdjustedAmt) / 100,
				TransactionId = ProcessorVoidResponse.ReversalResp.RefNum,
				AuthorizationCode = "ProcessorVoidNoAuthCode",
				TransactionType = paymentTransactionType,
				Last4Digits = creditCard.CardNumberLastFourDigits,
				CardType = creditCard.CardType,
				ExMonth = creditCard.ExpirationMonth,
				ExYear = creditCard.ExpirationYear
			};

			return _paymentGatewayTransactionRepository.Save(entry);
		}

		private Guid RecordTransaction(ProcessorCaptureResponse ProcessorCaptureResponse, PaymentTransactionType paymentTransactionType, CreditCardPartial creditCard)
		{
			var entry = new PaymentGatewayTransaction
			{
				PaymentGatewayProfileId = _paymentGatewayProfile.ProfileId,
				Amount = decimal.Parse(ProcessorCaptureResponse.MarkForCaptureResp.Amount) / 100,
				TransactionId = ProcessorCaptureResponse.MarkForCaptureResp.RefNum,
				AuthorizationCode = ProcessorCaptureResponse.MarkForCaptureResp.AuthCode,
				TransactionType = paymentTransactionType,
				Last4Digits = creditCard.CardNumberLastFourDigits,
				CardType = creditCard.CardType,
				ExMonth = creditCard.ExpirationMonth,
				ExYear = creditCard.ExpirationYear
			};

			return _paymentGatewayTransactionRepository.Save(entry);
		}

		private Guid RecordTransaction(ProcessorPaymentResponse ProcessorPaymentResponse, decimal amount, PaymentTransactionType paymentTransactionType, CreditCardPartial creditCard)
		{
			var entry = new PaymentGatewayTransaction
			{
				PaymentGatewayProfileId = _paymentGatewayProfile.ProfileId,
				Amount = amount,
				TransactionId = ProcessorPaymentResponse.NewOrderResp.RefNum,
				AuthorizationCode = ProcessorPaymentResponse.NewOrderResp.AuthCode,
				TransactionType = paymentTransactionType,
				Last4Digits = creditCard.CardNumberLastFourDigits,
				CardType = creditCard.CardType,
				ExMonth = creditCard.ExpirationMonth,
				ExYear = creditCard.ExpirationYear
			};

			return _paymentGatewayTransactionRepository.Save(entry);
		}

		private string Coalesce(string v1, string v2)
		{
			if (string.IsNullOrEmpty(v1))
			{
				return v2 ?? "";
			}
			return v1 ?? "";
		}

		private async Task<string> FlurlRequest(string xmlRequest, string url)
		{
			Dictionary<string, string> headers = _tokenExTransparentGatewayService.Headers;
			headers.Remove("TX_URL");
			var response = await url.WithHeaders(headers).PostStringAsync(xmlRequest).ReceiveString();
			return response;
		}

		private async Task<CreditCard> GetTokenizedCreditCard(PaymentRequest paymentRequest)
		{
			TokenizedCreditCard tokenizedCreditCard = paymentRequest.PaymentAccessor.GetTokenizedCreditCard();
			if (tokenizedCreditCard != null) // Saved Payment Method (token) was used at checkout
			{
				var cardVerificationValue = !string.IsNullOrEmpty(tokenizedCreditCard.CardVerificationValue) ? tokenizedCreditCard.CardVerificationValue.Trim() : null;
				return new CreditCard
				{
					AddressLine1 = tokenizedCreditCard.AddressLine1,
					CardNumber = tokenizedCreditCard.CardToken,
					CardType = tokenizedCreditCard.CardType,
					CardVerificationValue = cardVerificationValue,
					City = tokenizedCreditCard.City,
					ExpirationMonth = tokenizedCreditCard.CardExpirationMonth,
					ExpirationYear = tokenizedCreditCard.CardExpirationYear,
					FirstName = tokenizedCreditCard.FirstName,
					LastFourDigits = tokenizedCreditCard.LastFourDigits,
					LastName = tokenizedCreditCard.LastName,
					PostalCode = tokenizedCreditCard.PostalCode,
					State = tokenizedCreditCard.State,
					Country = Address.GetCountryCode(tokenizedCreditCard.State)
				};
			}
			// Raw card info was given at checkout
			CreditCard creditCard = paymentRequest.PaymentAccessor.GetCreditCard();
			creditCard.LastFourDigits = creditCard.LastFourDigits; // this forces logic to set last four before we change CC to a token
			creditCard.CardNumber = (await new TokenExTokenizer(_tokenizerConfig).GetTokenAsync(creditCard.CardNumber)).Token;
			return creditCard;
		}

		private enum TransactionType
		{
			[EnumMember(Value = "A")]
			Auth,
			[EnumMember(Value = "AC")]
			AuthCapture,
			[EnumMember(Value = "R")]
			Refund
		}

		private static string ToEnumString<T>(T instance)
		{
			if (!typeof(T).IsEnum) throw new ArgumentException("instance", "Must be enum type");
			string enumString = instance.ToString();
			var field = typeof(T).GetField(enumString);
			if (field != null) // instance can be a number that was cast to T, instead of a named value, or could be a combination of flags instead of a single value
			{
				var attr = (EnumMemberAttribute)field.GetCustomAttributes(typeof(EnumMemberAttribute), false)[0];
				if (attr != null) // if there's no EnumMember attr, use the default value
					enumString = attr.Value;
			}
			return enumString;
		}

		private string ProcessorXmlFormatting(string serializedXMLString)
		{
			// Remove namespaces and modify encoding directly in Serialized XML string
			return serializedXMLString.Replace(" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"", "")
				.Replace(" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"", "")
				.Replace("utf-16", "utf-8");
		}

		private bool IsToken(string data)
		{
			return Guid.TryParse(data, out Guid newGuid);
		}

		private class Log
		{
			public Log()
			{

			}

			public ProcessorPaymentResponse ProcessorPaymentResponse { get; set; }
			public ProcessorCaptureResponse ProcessorCaptureResponse { get; set; }
			public ProcessorVoidResponse ProcessorVoidResponse { get; set; }
			public PaymentTransactionType PaymentTransactionType { get; set; }
			public CreditCard CreditCard { get; set; }
			public VoidPaymentRequest VoidPaymentRequest { get; set; }
			public string OrderId { get; set; }
			public string Amount { get; set; }
			public string ResponseString { get; set; }
			public string RequestString { get; set; }

			private ProcessorErrorResponse _ProcessorErrorResponse;
			private XmlSerializer _deSerializer;
			private string _logMsg;

			public string LogTransaction()
			{
				if (ResponseString.Contains("QuickResp"))
				{
					_deSerializer = new XmlSerializer(typeof(ProcessorErrorResponse));
					using (var reader = new StringReader(ResponseString))
					{
						_ProcessorErrorResponse = (ProcessorErrorResponse)_deSerializer.Deserialize(reader);
					}
					_logMsg = LogTxn(_ProcessorErrorResponse);
					return _logMsg;
				}

				switch (PaymentTransactionType)
				{
					case PaymentTransactionType.Authorize:
					case PaymentTransactionType.Payment:
					case PaymentTransactionType.Refund:
						_deSerializer = new XmlSerializer(typeof(ProcessorPaymentResponse));
						using (var reader = new StringReader(ResponseString))
						{
							ProcessorPaymentResponse = (ProcessorPaymentResponse)_deSerializer.Deserialize(reader);
						}
						_logMsg = LogTxn(ProcessorPaymentResponse);
						break;
					case PaymentTransactionType.Capture:
						_deSerializer = new XmlSerializer(typeof(ProcessorCaptureResponse));
						using (var reader = new StringReader(ResponseString))
						{
							ProcessorCaptureResponse = (ProcessorCaptureResponse)_deSerializer.Deserialize(reader);
						}
						_logMsg = LogTxn(ProcessorCaptureResponse);
						break;
					case PaymentTransactionType.Void:
						_deSerializer = new XmlSerializer(typeof(ProcessorVoidResponse));
						using (var reader = new StringReader(ResponseString))
						{
							ProcessorVoidResponse = (ProcessorVoidResponse)_deSerializer.Deserialize(reader);
						}
						_logMsg = LogTxn(ProcessorVoidResponse);
						break;
					default:
						break;
				}
				return _logMsg;
			}

			private string LogTxn(object responseObject)
			{
				string formatString;
				string[] args;
				string logMsg;
				string respCode = "";
				switch (responseObject)
				{
					case ProcessorErrorResponse ProcessorErrorResponse:
						logMsg = string.Format("Processor {0}. ProcStatus: {1} - Message: {2} - Order #:{3} - Card Number: {4} - Amount: {5} - Request: {6}",
							PaymentTransactionType.GetDescription(),
							ProcessorErrorResponse.QuickResp.ProcStatus,
							ProcessorErrorResponse.QuickResp.StatusMsg,
							OrderId,
							CreditCard != null ? CreditCard.MaskedCardNumber : "Not Available",
							Amount != string.Empty ? Amount : "Not Available",
							RequestString);
						return logMsg;
					case ProcessorPaymentResponse ProcessorPaymentResponse:
						formatString = "Processor {0}. MerchantID: {1} - CardBrand: {2} - AccountNum: {3} - Order#: {4} - Amount: {5} - ";
						formatString = string.Concat(formatString, "AVSRespCode: {6} - HostAVSRespCode: {7} - AuthCode: {8} - StatusMsg: {9} - RefNum: {10} - ");
						formatString = string.Concat(formatString, "CVV2RespCode: {11} - HostCVV2RespCode: {12} - RespCode: {13} - Request: {14}");
						args = new string[15];
						args[0] = PaymentTransactionType.GetDescription();
						args[1] = ProcessorPaymentResponse.NewOrderResp.MerchantID;
						args[2] = ProcessorPaymentResponse.NewOrderResp.CardBrand;
						args[3] = ProcessorPaymentResponse.NewOrderResp.AccountNum;
						args[4] = ProcessorPaymentResponse.NewOrderResp.OrderID;
						args[5] = Amount;
						args[6] = ProcessorPaymentResponse.NewOrderResp.AVSRespCode;
						args[7] = ProcessorPaymentResponse.NewOrderResp.HostAVSRespCode;
						args[8] = ProcessorPaymentResponse.NewOrderResp.AuthCode;
						args[9] = ProcessorPaymentResponse.NewOrderResp.StatusMsg;
						args[10] = ProcessorPaymentResponse.NewOrderResp.RefNum;
						args[11] = ProcessorPaymentResponse.NewOrderResp.CVV2RespCode;
						args[12] = ProcessorPaymentResponse.NewOrderResp.HostCVV2RespCode;
						args[13] = ProcessorPaymentResponse.NewOrderResp.RespCode;
						args[14] = RequestString;
						logMsg = string.Format(formatString, args);
						Logger.Current.Write(logMsg, TraceEventType.Information);
						respCode = ProcessorPaymentResponse.NewOrderResp.RespCode;
						break;
					case ProcessorVoidResponse ProcessorVoidResponse:
						logMsg = string.Format("Processor {0}. MerchantID: {1} - OrderID: {2} - AuthorizedAmount: {3} - StatusMsg: {4} - RefNum: {5} - Request: {6}",
						PaymentTransactionType.GetDescription(),
						ProcessorVoidResponse.ReversalResp.MerchantID,
						ProcessorVoidResponse.ReversalResp.OrderID,
						VoidPaymentRequest.AuthorizedAmount,
						ProcessorVoidResponse.ReversalResp.StatusMsg,
						ProcessorVoidResponse.ReversalResp.RefNum,
						RequestString);
						Logger.Current.Write(logMsg, TraceEventType.Information);
						break;
					case ProcessorCaptureResponse ProcessorCaptureResponse:
						formatString = "Processor {0}. MerchantID: {1} - Order#: {2} - Amount: {3} - ProcStatus: {4} - ApprovalStatus: {5} - ";
						formatString = string.Concat(formatString, "AVSRespCode: {6} - HostAVSRespCode: {7} - AuthCode: {8} - RefNum: {9} - ");
						formatString = string.Concat(formatString, "RespCode: {10} - Request: {10}");
						args = new string[12];
						args[0] = PaymentTransactionType.GetDescription();
						args[1] = ProcessorCaptureResponse.MarkForCaptureResp.MerchantID;
						args[2] = ProcessorCaptureResponse.MarkForCaptureResp.OrderID;
						args[3] = Amount;
						args[4] = ProcessorCaptureResponse.MarkForCaptureResp.ProcStatus;
						args[5] = ProcessorCaptureResponse.MarkForCaptureResp.ApprovalStatus;
						args[6] = ProcessorCaptureResponse.MarkForCaptureResp.AVSRespCode;
						args[7] = ProcessorCaptureResponse.MarkForCaptureResp.HostAVSRespCode;
						args[8] = ProcessorCaptureResponse.MarkForCaptureResp.AuthCode;
						args[9] = ProcessorCaptureResponse.MarkForCaptureResp.RefNum;
						args[10] = ProcessorCaptureResponse.MarkForCaptureResp.RespCode;
						args[11] = RequestString;
						logMsg = string.Format(formatString, args);
						Logger.Current.Write(logMsg, TraceEventType.Information);
						respCode = ProcessorCaptureResponse.MarkForCaptureResp.RespCode;
						break;
					default:
						break;
				}

				if (!new ProcessorApprovalCodes().IsProcessorApprovalCode(respCode))
                {
					return respCode;
                }
				return "";
			}
		}
	}
}