<%	
dim sub_relation_txt,cardholderrelation_txt,subcardnumber,SubExpirydate,CardExpirydate,CardExpirydate2

if Res_sale("CARDHOLDERNAME") <> "" then
	sub_relation_txt = Split(Res_sale("CARDHOLDERNAME"),"(")
	response.write sub_relation_txt(0)
	cardholderrelation_txt = Mid(sub_relation_txt(1),1,(InStr(sub_relation_txt(1),")")-1))
end if

if Res_sale("CARDNUMBER") <> "" then
	subcardnumber = Mid(Res_sale("CARDNUMBER"),1,4)&"XXXXXXXXXXXX"
end if

if Res_sale("CARDEXPIRYDATE") <> "" then
	SubExpirydate = Split(Res_sale("CARDEXPIRYDATE"),"/")
	CardExpirydate = SubExpirydate(1)
	CardExpirydate2 = Mid(SubExpirydate(2),3,2)

end if


response.write "<table width='100%' cellpadding='0' cellspacing='0'>"

response.write "<thead>" & _
					"<tr height='25px'>" & _
						"<th style='text-align:center; width:50%;' colspan='2'>ข้อมูลเดิม</th>" & _
						"<th style='text-align:center; width:50%;' colspan='2'>ข้อมูลใหม่</th>" & _
					"</tr>" & _
				"</thead>"
				
response.write "<tbody>"
response.write "<tr style='height:30px;' bgcolor='#CCCCCC'>" & _
					"<td colspan='2'><strong>ข้อมูลบัตรเครดิต</strong></td>" & _
					"<td colspan='2'><strong>ข้อมูลบัตรเครดิต</strong></td>" & _
				"</tr>"
				
response.write "<input type='hidden' id='GenReceiveCost' name='GenReceiveCost' value='' />"
response.write "<tr style='height:30px;'>" & _
					"<td style='width:15%;'><strong>ธนาคาร</strong></td>" & _
					"<td>" & _
						replace(getCombo("N_CARDBANKID_OLD", "cmbBank",Res_sale("CARDBANKID")), "<select ", "<select style='width:210px; height:25px;' disabled ") & _
					"</td>" & _
					"<td style='width:15%;'><strong>ธนาคาร</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='N_CARDBANKIDOLD' name='N_CARDBANKIDOLD' value='"& Res_sale("CARDBANKID") & "'/>" & _
						replace(getCombo("N_CARDBANKIDNEW", "cmbBank",""), "<select ", "<select style='width:auto; height:25px;' onchange=""genCostcode(0,0)""") & _
					"</td>" & _
				"</tr>"
				
response.write "<tr style='height:30px;'>" & _
					"<td><strong>ชื่อผู้ถือบัตร</strong></td>" & _
					"<td>" & _
						"<input type='text' name='C_CARDHOLDERNAME_OLD' id='C_CARDHOLDERNAME_OLD' value='"& Res_sale("CARDHOLDERNAME") &"' style='width:210px; height:20px;' disabled />" & _
					"</td>" & _
					"<td><strong>ชื่อผู้ถือบัตร</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='C_CARDHOLDERNAMEOLD' name='C_CARDHOLDERNAMEOLD' value='"& Res_sale("CARDHOLDERNAME") &"'/>" & _
						"<input type='text' name='C_CARDHOLDERNAMENEW' id='C_CARDHOLDERNAMENEW' value='' maxlength='255' style='width:210px; height:20px;' onkeypress=""return key_word(event);"" />" & _
					"</td>" & _
				"</tr>"
				
				
response.write "<tr style='height:30px;'>" & _
					"<td style='width:13%;'><strong>ความสัมพันธ์</strong></td>" & _
					"<td>" & _
						replace(getCombo("C_CARDHOLDERRELATION_OLD", "cmbCardholderRelation",cardholderrelation_txt), "<select ", "<select style='width:210px; height:25px;' disabled ") & _
					"</td>" & _
					"<td style='width:13%;'><strong>ความสัมพันธ์</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='C_CARDHOLDERRELATIONOLD' name='C_CARDHOLDERRELATIONOLD' value='"& cardholderrelation_txt &"' />" & _
						replace(getCombo("C_CARDHOLDERRELATIONNEW", "cmbCardholderRelation",""), "<select ", "<select style='width:213px; height:25px;'") & _
					"</td>" & _
				"</tr>"
				
response.write "<tr style='height:30px;'>" & _
					"<td ></td>" & _
					"<td>" & _
						"<input type='text' name='CARDHOLDERRELATION_TXT_OLD' id='CARDHOLDERRELATION_TXT_OLD' value='"& cardholderrelation_txt & "' style='width:210px; height:20px;' disabled />" & _
					"</td>" & _
					"<td></td>" & _
					"<td>" & _
						"<input type='hidden' id='CARDHOLDERRELATION_TXTOLD' name='CARDHOLDERRELATION_TXTOLD' value='"& cardholderrelation_txt &"' />" & _
						"<input type='text' name='CARDHOLDERRELATION_TXTNEW' id='CARDHOLDERRELATION_TXTNEW' value='' maxlength='255' style='width:210px; height:20px;' onkeypress=""return key_word(event);"" />" & _
					"</td>" & _
				"</tr>"
		


response.write "<tr style='height:30px;'>" & _
					"<td><strong>บัตรเครดิต</strong></td>" & _
					"<td>" & _
						"<input type='radio' name='C_CARDTYPE_OLD' id='C_CARDTYPE_OLD' value='visa' "& getchecked("visa",Res_sale("CARDTYPE")) &" disabled /> <strong>VISA</strong>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" & _
						"<input type='radio' name='C_CARDTYPE_OLD' id='C_CARDTYPE_OLD' value='master' "& getchecked("master",Res_sale("CARDTYPE")) &" disabled /> <strong>MASTER</strong>&nbsp;&nbsp;&nbsp;&nbsp;" & _
						"<input type='radio' name='C_CARDTYPE_OLD' id='C_CARDTYPE_OLD' value='american' "& getchecked("american",Res_sale("CARDTYPE")) &" disabled /> <strong>AMERICAN</strong>" & _
					"</td>" & _
					"<td><strong>บัตรเครดิต</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='C_CARDTYPEOLD' name='C_CARDTYPEOLD' value='"& Res_sale("CARDTYPE") &"' />" & _
						"<input type='radio' name='C_CARDTYPENEW' id='C_CARDTYPENEW' value='visa' /><strong>VISA</strong>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" & _
						"<input type='radio' name='C_CARDTYPENEW' id='C_CARDTYPENEW' value='master' /><strong>MASTER</strong>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" & _
						"<input type='radio' name='C_CARDTYPENEW' id='C_CARDTYPENEW' value='american' /><strong>AMERICAN</strong>" & _
					"</td>" & _
				"</tr>"
				
response.write "<tr style='height:30px;'>" & _
					"<td>&nbsp;</td>" & _
					"<td>" & _
						"<input type='radio' name='C_CARDTYPE_OLD' id='C_CARDTYPE_OLD' value='dinner' "& getchecked("dinner",Res_sale("CARDTYPE")) &" disabled /> <strong>DINNER</strong>&nbsp;" & _
						"<input type='radio' name='C_CARDTYPE_OLD' id='C_CARDTYPE_OLD' value='jcb' "& getchecked("jcb",Res_sale("CARDTYPE")) &" disabled /> <strong>JCB</strong>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" & _
						"<input type='radio' name='C_CARDTYPE_OLD' id='C_CARDTYPE_OLD' value='bank' "& getchecked("bank",Res_sale("CARDTYPE")) &" disabled /> <strong>-N/A-BANK-</strong>" & _
					"</td>" & _
					"<td>&nbsp;</td>" & _
					"<td>" & _
						"<input type='hidden' id='C_CARDTYPEOLD' name='C_CARDTYPEOLD' value='"& Res_sale("CARDTYPE") &"' />" & _
						"<input type='radio' name='C_CARDTYPENEW' id='C_CARDTYPENEW' value='dinner' /><strong>DINNER</strong>&nbsp;" & _
						"<input type='radio' name='C_CARDTYPENEW' id='C_CARDTYPENEW' value='jcb' /><strong>JCB</strong> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;" & _
						"<input type='radio' name='C_CARDTYPENEW' id='C_CARDTYPENEW' value='bank' /><strong>-N/A-BANK-</strong>" & _
					"</td>" & _
				"</tr>"

response.write "<tr style='height:30px;'>" & _
					"<td><strong>เลขที่บัตรเครดิต</strong></td>" & _
					"<td>" & _
						"<input type='text' name='C_CARDNUMBER_OLD' id='C_CARDNUMBER_OLD' value='"& subcardnumber &"' style='width:210px; height:20px;' disabled />" & _
					"</td>" & _
					"<td><strong>เลขที่บัตรเครดิต</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='C_CARDNUMBEROLD' name='C_CARDNUMBEROLD' value='"& Res_sale("CARDNUMBER") &"' />" & _
						"<input type='text' name='C_CARDNUMBERNEW' id='C_CARDNUMBERNEW' value='' style='width:210px; height:20px;' onkeypress=""return key_num(event);"" maxlength='16' />" & _
					"</td>" & _
				"</tr>"
				
response.write "<tr style='height:30px;'>" & _
					"<td><strong>วันหมดอายุ (เดือน/ปี)</strong></td>" & _
					"<td>" & _
						"<input type='text' name='C_CARDEXPIRYDATE1_OLD' id='C_CARDEXPIRYDATE1_OLD' value='"& CardExpirydate &"' style='width:100px; height:20px;' disabled />/" & _
						"<input type='text' name='C_CARDEXPIRYDATE2_OLD' id='C_CARDEXPIRYDATE2_OLD' value='"& CardExpirydate2 &"' style='width:100px; height:20px;' disabled />" & _
					"</td>" & _
					"<td><strong>วันหมดอายุ (เดือน/ปี)</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='C_CARDEXPIRYDATEOLD1' name='C_CARDEXPIRYDATEOLD1' value='"& CardExpirydate &"' />" & _
						"<input type='hidden' id='C_CARDEXPIRYDATEOLD2' name='C_CARDEXPIRYDATEOLD2' value='"& CardExpirydate2 &"' />" & _
						"<input type='text' name='C_CARDEXPIRYDATENEW1' id='C_CARDEXPIRYDATENEW1' value='' style='width:100px; height:20px;' maxlength='2' />/" & _
						"<input type='text' name='C_CARDEXPIRYDATENEW2' id='C_CARDEXPIRYDATENEW2' value='' style='width:100px; height:20px;' maxlength='2' />" & _
					"</td>" & _
				"</tr>"
				
response.write "<tr><td colspan='4'><h1></h1></td></tr>"

response.write "<tr style='height:30px;'>" & _
					"<td colspan='2' bgcolor='#CCCCCC'><strong>ข้อมูลการหักบัญชี</strong></td>" & _
					"<td colspan='2' bgcolor='#CCCCCC'><strong>ข้อมูลการหักบัญชี</strong></td>" & _
				"</tr>"
				
response.write "<tr style='height:30px;'>" & _
					"<td><strong>ธนาคาร</strong></td>" & _
					"<td>" & _
						replace(getCombo("N_BANKID_OLD", "cmbBank",Res_sale("BANKID")), "<select ", "<select style='width:210px; height:25px;' disabled ") & _
					"</td>" & _
					"<td><strong>ธนาคาร</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='N_BANKIDOLD' name='N_BANKIDOLD' value='"& Res_sale("BANKID") &"' />" & _
						replace(getCombo("N_BANKIDNEW", "cmbBank",""), "<select ", "<select style='width:210px; height:25px;'") & _
					"</td>" & _
				"</tr>"
				
response.write "<tr style='height:30px;'>" & _
					"<td><strong>สาขา</strong></td>" & _
					"<td>" & _
						"<input type='text' name='C_BANKBRANCH_OLD' id='C_BANKBRANCH_OLD' value='"& Res_sale("BANKBRANCH") &"' style='width:210px; height:20px;' disabled />" & _
					"</td>" & _
					"<td><strong>สาขา</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='C_BANKBRANCHOLD' name='C_BANKBRANCHOLD' value='" &Res_sale("BANKBRANCH") &"' />" & _
						"<input type='text' name='C_BANKBRANCHNEW' id='C_BANKBRANCHNEW' value='' maxlength='255' style='width:210px; height:20px;' />" & _
					"</td>" & _
				"</tr>"
		
response.write "<tr style='height:30px;'>" & _
					"<td><strong>ชื่อบัญชี</strong></td>" & _
					"<td>" & _
						"<input type='text' name='C_BANKACCOUNTNAME_OLD' id='C_BANKACCOUNTNAME_OLD' value='"& Res_sale("BANKACCOUNTNAME") &"' style='width:210px; height:20px;' disabled />" & _
					"</td>" & _
					"<td><strong>ชื่อบัญชี</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='C_BANKACCOUNTNAMEOLD' name='C_BANKACCOUNTNAMEOLD' value='"& Res_sale("BANKACCOUNTNAME") &"' />" & _
						"<input type='text' name='C_BANKACCOUNTNAMENEW' id='C_BANKACCOUNTNAMENEW' value='' maxlength='255' style='width:210px; height:20px;' />" & _
					"</td>" & _
				"</tr>"
		
response.write "<tr style='height:30px;'>" & _
					"<td><strong>ธนาคาร</strong></td>" & _
					"<td>" & _
						replace(getCombo("C_BANKACCOUNTTYPE_OLD", "cmbBankaccounttype",Res_sale("BANKACCOUNTTYPE")), "<select ", "<select style='width:210px; height:25px;' disabled ") & _
					"</td>" & _
					"<td><strong>ธนาคาร</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='C_BANKACCOUNTTYPEOLD' name='C_BANKACCOUNTTYPEOLD' value='"& Res_sale("BANKACCOUNTTYPE") &"' />" & _
						replace(getCombo("C_BANKACCOUNTTYPENEW", "cmbBankaccounttype",""), "<select ", "<select style='width:210px; height:25px;'") & _
					"</td>" & _
				"</tr>"
				
response.write "<tr style='height:30px;'>" & _
					"<td><strong>เลขที่บัญชี</strong></td>" & _
					"<td>" & _
						"<input type='text' name='C_BANKACCOUNT_OLD' id='C_BANKACCOUNT_OLD' value='"& Res_sale("BANKACCOUNT") &"' style='width:210px; height:20px;' disabled />" & _
					"</td>" & _
					"<td><strong>เลขที่บัญชี</strong></td>" & _
					"<td>" & _
						"<input type='hidden' id='C_BANKACCOUNTOLD' name='C_BANKACCOUNTOLD' value='" &Res_sale("BANKACCOUNT") &"' />" & _
						"<input type='text' name='C_BANKACCOUNTNEW' id='C_BANKACCOUNTNEW' value='' maxlength='10' style='width:210px; height:20px;' onkeypress=""return key_num(event);""/>" & _
					"</td>" & _
				"</tr>"
				
response.write "</tbody>"
response.write "</table><br>"

'*--------------------------------------------------- แผนการชำระ
response.write "<br><br><br><table width='100%' class='grid' cellpadding='0' cellspacing='0'><tr><td><h1></h1></td></tr></table>"
dim sqlPayment,objPayment,sqlPaymentmode,objPaymentmode
dim amount,balance,i
    sqlPayment = "select p.* ,to_char(p.duedate,'dd/mm/yy') duedate ,p.receivecostcode ,(select r.receivecostname from xininsure.receivecost r where 	r.receivecostcode = p.receivecostcode) receivecostname,(select bytedes from xininsure.sysbytedes s where s.tablename = 'PAYMENT' and s.columnname = 'PAYMENTTYPE' and s.bytecode = p.paymenttype) bytedes from xininsure.salepayment p where p.saleid = "& request("saleid") &" order by p.installment"
	Set objPayment = objConn.Execute(sqlPayment)

response.write "<table width='100%' class='grid' cellpadding='0' cellspacing='0'>"
response.write "<thead>" & _
					"<tr height='25px'>" & _
						"<th style='text-align:center; width:30%;' colspan='7'>ข้อมูลเดิม</th>" & _
					"</tr>" & _
				"</thead>"
response.write "<tbody>" & _
					"<tr style='height:30px;'>" & _
						"<td style='text-align:center; width:5%;'><strong>งวดที่</strong></td>" & _
						"<td style='text-align:center; width:15%;'><strong>TYPE</strong></td>" & _
						"<td style='text-align:center; width:15%;'><strong>นัดชำระ</strong></td>" & _
						"<td style='text-align:right; width:15%;'><strong>ยอดเงิน</strong></td>" & _
						"<td style='text-align:right; width:15%;'><strong>คงค้าง</strong></td>" & _
						"<td style='text-align:center; width:100%;'><strong>Cost Code</strong></td>" & _
					"</tr>"
	if not objPayment.eof then
		amount = 0
		balance = 0
		i = 0
		do until objPayment.eof
			amount = amount + CDbl(objPayment("amount"))
			balance = balance +  CDbl(objPayment("balance"))
			response.write "<tr style='height:30px;'>" & _
								"<td align='center'>"& objPayment("installment") &"</td>" & _
								"<td>"& objPayment("paymenttype") &" : "& objPayment("bytedes") &"</td>" & _
								"<td align='center'>"& objPayment("duedate") &"</td>" & _
								"<td align='right'>"& formatnumber(objPayment("amount")) &"</td>" & _
								"<td align='right'>"& formatnumber(objPayment("balance")) &"</td>" & _
								"<td align='center'>"
									if objPayment("receivecostcode") <> "" then 
									response.write objPayment("receivecostcode") &" : "& objPayment("receivecostname") 
									end if
			response.write "</td>"
			response.write "</tr>"
			
			i = i+1
		objPayment.movenext
		loop
		response.write "<tr style='height:30px;background-color:#F1ECFA'>" & _
								"<td>&nbsp;</td>" & _
								"<td><strong>รวม</strong></td>" & _
								"<td>&nbsp;</td>" & _
								"<td align='right'><strong>"& formatnumber(amount) &"</strong></td>" & _
								"<td align='right'><strong>"& formatnumber(balance) &"</strong></td>" & _
								"<td>&nbsp;</td>" & _
								"<td>&nbsp;</td>" & _
							"</tr>"
	else
		response.write "<table width='100%' class='grid' cellpadding='0' cellspacing='0'>"
			response.write "<thead>" & _
							"<tr height='25px'>" & _
								"<th style='text-align:center; width:30%;' colspan='7'>ไม่พบข้อมูลแผนการชำระ</th>" & _
							"</tr>" & _
						"</thead></table>"
	end if
response.write "</tbody>"
response.write "</table>"


response.write "<div style='width:98%; margin-right:0px; text-align:right;'>"
response.write "<input type='hidden' name='hdn_paymenttype' id='hdn_paymenttype' value='' />"
response.write "<p id='install_pay'>" & _
					"<strong>เลือกชำระ</strong> " & _
					"<input type='hidden' id='N_INSTALLMENTOLD' name='N_INSTALLMENTOLD' value='"& i &"' />" & _
					"<select name='INSTALLMENTNEW' id='INSTALLMENTNEW' onchange=""GetInstall(this.value)"">" & _
						"<option value='0'>เลือก</option>"
						Dim y
						For y = 1 To 6
							response.write "<option value='"& y &"'>"& y &"</option>"
						Next
	response.write "</select>" & _
					" <strong>งวด</strong>" & _
				"</p>"
response.write "</div>"
response.write "<div id='payment_new'></div>"

%>
<script>
	function GetInstall(paymentmode){
		var ProductID = "<%=Res_sale("productid")%>";
		var saleid = "<%=request("saleid")%>";
		var Installmentold = $("#N_INSTALLMENTOLD").val();

		$.ajax({
			url  	: "payment_freeform.asp",
			data 	: {paymentmode:paymentmode,ProductID:ProductID,saleid:saleid,Installmentold:Installmentold},
			type 	: "POST",
			success: function(result){
				$('#payment_new').html(result);
			}
		});
		
	}
</script>