<%	
	dim objConn
	dim i,productnetamount,j,x,installment
	dim Sql_Payment,Res_Payment,avgamount
	Set objConn = Server.CreateObject("ADODB.Connection")
	objConn.Open Application("DSN")

	Sql_Payment = "SELECT P.*,TO_CHAR(P.DUEDATE,'DD/MM/YYYY') DUEDATE,(select netamount from xininsure.sale s where s.saleid = p.saleid) productnetamount FROM xininsure.SALEPAYMENT P WHERE P.SALEID = "& request("saleid") &" ORDER BY P.INSTALLMENT"
	Set Res_Payment = objConn.Execute(Sql_Payment)

	cardbankid = request("cardbankid")

	response.write "<table width='100%' cellpadding='0' cellspacing='0'>"
	response.write "<tr><td colspan='7'><h1></h1></td></tr>" & _
					"<thead>" & _
						"<tr class='grid-row inline_editors grid-row-even' height='25px'>" & _
							"<th colspan='8'>ข้อมูลใหม่</th>" & _
						"</tr>" & _
					"</thead>"
	response.write "<tbody>" & _
					"<tr style='height:30px;'>" & _
						"<td style='text-align:center; width:5%;'><strong>งวดที่</strong></td>" & _
						"<td style='text-align:center; width:15%;'><strong>TYPE</strong></td>" & _
						"<td style='text-align:center; width:15%;'><strong>นัดชำระ</strong></td>" & _
						"<td style='text-align:right; width:15%;'><strong>ยอดเงิน</strong></td>" & _
						"<td style='text-align:center; width:15%;'><strong>Cost Code</strong></td>" & _
					"</tr>"
					

					
		if request("paymentmode") <> "" then
			
			installment = request("paymentmode")
			avgamount = Res_Payment("productnetamount")/installment

			productnetamount = 0
			For x = 1 To installment
				Response.Write "<tr style='height:30px;'>"

				' Column 1: INSTALLMENT
				Response.Write "<td style='text-align:center; width:5%;'>"
				Response.Write "<input type='hidden' name='NETAMOUNT" & x & "' id='NETAMOUNT" & x & "' value='" & Res_Payment("productnetamount") & "' />"
				Response.Write "<input type='hidden' name='PM_N_INSTALLMENTOLD" & x & "' id='PM_N_INSTALLMENTOLD" & x & "' value='" & Res_Payment("INSTALLMENT") & "' />"
				Response.Write "<input type='hidden' name='PM_N_INSTALLMENTNEW" & x & "' id='PM_N_INSTALLMENTNEW" & x & "' value='" & x & "' />"
				Response.Write x
				Response.Write "</td>"

				' Column 2: PAYMENTTYPE
				Response.Write "<td style='text-align:center; width:15%;'>"
				Response.Write "<input type='hidden' name='PM_C_PAYMENTTYPEOLD" & x & "' id='PM_C_PAYMENTTYPEOLD" & x & "' value='" & Res_Payment("PAYMENTTYPE") & "' />"
				Response.Write replace(getCombo("PM_C_PAYMENTTYPENEW"&x&"", "cmbPaymentType",""),"<select","<select onchange=""genCostcode("&x&",this.value)"" style='width:150px; height:25px;'")
				Response.Write "</td>"

				' Column 3: DUE DATE
				Response.Write "<td class='grid-cell char'>"
				Response.Write "<input type='hidden' name='PM_D_DUEDATEOLD" & x & "' id='PM_D_DUEDATEOLD" & x & "' value='" & Res_Payment("DUEDATE") & "' />"
				Response.Write "<input type='text' name='PM_D_DUEDATENEW" & x & "' id='PM_D_DUEDATENEW" & x & "' value='' style='width:140px; height:20px;' placeholder='dd/mm/yyyy' maxlength='10' />"
				Response.Write "</td>"

				' Column 4: AMOUNT
				Response.Write "<td class='grid-cell char' align=right>"
				Response.Write "<input type='hidden' name='PM_N_AMOUNTOLD" & x & "' id='PM_N_AMOUNTOLD" & x & "' value='" & FormatNumber(Res_Payment("AMOUNT"), 2) & "' />"

				If i <> installment Then
					Response.Write "<input type='text' name='PM_N_AMOUNTNEW" & x & "' id='PM_N_AMOUNTNEW" & x & "' value='"& FormatNumber(avgamount,2) &"' maxlength='13' style='width:140px; height:20px; text-align:right;' "
					Response.Write "onblur='blurnumfloat(this)' onchange='Cal_amount()' onkeypress='return key_cover(event);' />"
				Else
					Response.Write "<input type='hidden' name='PM_N_AMOUNTNEW" & x & "' id='PM_N_AMOUNTNEW" & x & "' value='' maxlength='13' style='width:140px; height:20px; text-align:right;' onchange='Cal_amount()' />"
					Response.Write "<p id='Cal_Amount' style='color:#F00; font-weight: bold; width:140px; text-align:right'>"& FormatNumber(avgamount,2) &"</p>"
				End If
				Response.Write "</td>"

				' Column 5: COST CODE
				Response.Write "<td class='grid-cell char'>"
				Response.Write "<td style='text-align:right; width:15%;'>" & _
									"<input type='hidden' name='PM_C_RECEIVECOSTCODEOLD"& x &"' id='PM_C_RECEIVECOSTCODEOLD"& x &"' value='"& Res_Payment("receivecostcode") &"' />" & _
									"<select name='PM_C_RECEIVECOSTCODENEW"& x &"' id='PM_C_RECEIVECOSTCODENEW"& x &"' style='width: 250px'><option value=>** กรุณาเลือก cost code **</option></select>" & _
								"</td>"

				Response.Write "</tr>"
				
				productnetamount = Res_Payment("productnetamount")
				
			Next
			
			response.write "<tr style='height:30px;background-color:#F1ECFA'>" & _
								"<td>&nbsp;</td>" & _
								"<td><strong>ยอดชำระทั้งสิ้น</strong></td>" & _
								"<td>" & _
									"<input type='hidden' id='countamount' name='countamount' value='"& formatnumber(productnetamount) &"' />" & _
								"</td>" & _
								"<td style='text-align:right;font-weight:bold;'>"& formatnumber(productnetamount) &"</td>" & _
								"<td>&nbsp;</td>" & _
								"<td>&nbsp;</td>" & _
							"</tr>"
							
		end if	
		response.write "<input type='hidden' name='TOTALINSTALLMENTNEW' id='TOTALINSTALLMENTNEW' value='"& installment &"' />"
		response.write "<input type='hidden' name='TOTALINSTALLMENTOLD' id='TOTALINSTALLMENTOLD' value='"& request("Installmentold") &"' />"
		response.write "<input type='hidden' name='N_INSTALLMENTNEW' id='N_INSTALLMENTNEW' value='"& installment &"' />"		
	response.write "</tbody>"
	response.write "</table>"
	
	response.write "<input type='hidden' name='countinstall' id='countinstall' value='"&installment&"' />"
	
function getCombo(cmbName, RecordSource, SetValue)
	dim strx

	strx= Replace(application(RecordSource),"sysvalue",cmbName & " id=" & cmbName)
	if SetValue <> "" then
	   strx = Replace(strx,"value=" & SetValue, "value=" & SetValue & " selected")
	end if
	getCombo = strx

end Function
%>
<script>

$(function(){
	var loopInstall = $("#TOTALINSTALLMENTNEW").val();
	var i = 1;
	var flg;
	var policydate;
	var var_policy;
	var val_policydat;
	
	if($("#D_POLICYDATENEW").val() === ""){
		policydate = $("#D_POLICYDATEOLD").val();
		strdate = policydate.split('/');
		var_policy = new Date(strdate[2],parseInt(strdate[1])-1,parseInt(strdate[0])+1);
		var_policy.setDate(var_policy.getDate()+ 32)
		val_policydate =((var_policy.getDate())<=9?"0"+(var_policy.getDate()):(var_policy.getDate())) + '/' + ((var_policy.getMonth()+1)<=9?"0"+(var_policy.getMonth()+1):(var_policy.getMonth()+1)) + '/' + (var_policy.getFullYear());
	}else{
		policydate = $("#D_POLICYDATENEW").val();
		strdate = policydate.split('/');
		var_policy = new Date(strdate[2],parseInt(strdate[1])-1,parseInt(strdate[0])+1);
		var_policy.setDate(var_policy.getDate()+ 32)
		val_policydate =((var_policy.getDate())<=9?"0"+(var_policy.getDate()):(var_policy.getDate())) + '/' + ((var_policy.getMonth()+1)<=9?"0"+(var_policy.getMonth()+1):(var_policy.getMonth()+1)) + '/' + (var_policy.getFullYear());		
	}
	
	//alert($("#D_POLICYDATEOLD").val()+"---"+ $("#D_POLICYDATENEW").val())
	//alert(var_policy+"---"+val_policydate)
	for(i;i<=loopInstall;i++){
		if(document.getElementById("PM_C_PAYMENTTYPENEW"+i).value == 'K'){
			flg = +0;
		}else{
			flg = +1;
		}

		$("#PM_D_DUEDATENEW"+i).datepicker({
			dateFormat : 'dd/mm/yy',
			changeMonth: true, 
			changeYear : true,
			monthNamesShort: ['มกราคม','กุมภาพันธ์','มีนาคม','เมษายน','พฤษภาคม','มิถุนายน','กรกฎาคม','สิงหาคม','กันยายน','ตุลาคม','พฤศจิกายน','ธันวาคม'],
			minDate: flg,
			maxDate: val_policydate
		});
	}
	genCostcode(0,0);
});

function Cal_amount(){
	var SaleNetAmount = <%=productnetamount%>;	
	var loopInstall =  $("#N_INSTALLMENTNEW").val();
	var SumAmount = 0;
	var amount = 0;
	var x = 1;

	for(x;x<=loopInstall;x++){
		if(x != loopInstall){
			amount = returnzero(uncommas($("#PM_N_AMOUNTNEW"+x).val()));
			SumAmount = parseFloat(SumAmount) + parseFloat(amount);
		}else{
			SumAmount = parseFloat(SaleNetAmount) - parseFloat(SumAmount);
			$('#PM_N_AMOUNTNEW'+x).val(addCommas(SumAmount.toFixed(2)));		
		}			
	}
	$('#Cal_Amount').html(addCommas(SumAmount.toFixed(2)));
	SumAmountPayment();
}

function SumAmountPayment(){
	var Installrun =  $("#N_INSTALLMENTNEW").val();
	var z = 1;
	var PaymentAmount = 0;
	var CountAmountPayment = 0;
	for(z;z<=Installrun;z++){
		PaymentAmount = returnzero(uncommas($("#PM_N_AMOUNTNEW"+z).val()));
		CountAmountPayment = parseFloat(CountAmountPayment) + parseFloat(PaymentAmount);
	}
	$('#SumAmountPayment').html(addCommas(CountAmountPayment.toFixed(2)));//ใช้แสดง
	$('#countamount').val(CountAmountPayment);//ใช้แสดง
}

function genCostcode(install,receive){

	var installrun = $("#N_INSTALLMENTNEW").val();// จำนวนงวด
	var corpstatus = $("#CORPSTATUS").val();// จำนวนงวด
	var countst = 1;
	var GenReceiveCostCode = "<%=application("strRECEIVECOST")%>";
	var arr_Receive = GenReceiveCostCode.split('|');
	var banknew = $("#N_CARDBANKIDNEW").val();// ธนาคาร
	var bankold = $("#N_CARDBANKIDOLD").val();// ธนาคาร
	var bank;// ธนาคาร
	banknew==''? bank=bankold:bank=banknew;
	
	if(receive == "0"){
		for(var x=1;x<=installrun;x++){
			$("#PM_C_RECEIVECOSTCODENEW"+x).children('option:not(:first)').remove();
			if($("#PM_C_PAYMENTTYPENEW"+x).val() == "R" || $("#PM_C_PAYMENTTYPENEW"+x).val() == "K"){
				$("#PM_C_RECEIVECOSTCODENEW"+x).append("<option value='TQMFULL'>ชำระเต็มงวดเดียว</option>");
				$("#PM_C_RECEIVECOSTCODENEW"+x).append("<option value='TQMFULL01'>ชำระเต็มงวดเดียว (Confirm)</option>");
				for(var xx=0;xx<arr_Receive.length;xx++){
					if(arr_Receive[xx] != ""){
						var cost = arr_Receive[xx].split('*');
						if(cost[3] == bank){
							$("#PM_C_RECEIVECOSTCODENEW"+x).append("<option value='"+cost[0]+"'>"+cost[1]+"</option>");
						}
					}
				}
			}
		}
	}else if(receive == "R" || receive == "K"){
		$("#PM_C_RECEIVECOSTCODENEW"+install).children('option:not(:first)').remove();
		for(var y=0;y<arr_Receive.length;y++){
			if(arr_Receive[y] != ""){
				var cost = arr_Receive[y].split('*');
				if(cost[3] == ""){//bankid
					if(cost[0] == "TQMFULL"){
						$("#PM_C_RECEIVECOSTCODENEW"+install).append("<option value='"+cost[0]+"'>"+cost[1]+"</option>");
						countst=countst+1;
					}else if(cost[0] == "TQMFULL01"){
						$("#PM_C_RECEIVECOSTCODENEW"+install).append("<option value='"+cost[0]+"'>"+cost[1]+"</option>");
						countst=countst+1;
					}
				}else{
					if(cost[3] == bank){//bankid
						$("#PM_C_RECEIVECOSTCODENEW"+install).append("<option value='"+cost[0]+"'>"+cost[1]+"</option>");
					}
				}
			}
		}
	}
	/*else if(receive == "B"){
		banknew = '384';// ธนาคาร
		$("#PM_C_RECEIVECOSTCODENEW"+install).children('option:not(:first)').remove();
		for(var y=0;y<arr_Receive.length;y++){
			if(arr_Receive[y] != ""){
				var cost = arr_Receive[y].split('*');
				if(cost[3] == bank){//bankid
					$("#PM_C_RECEIVECOSTCODENEW"+install).append("<option value='"+cost[0]+"'>"+cost[1]+"</option>");
				}
			}
		}
	}*/

	else if(receive == "B" || receive == "G"){
		banknew = '384';// ธนาคาร
		$("#PM_C_RECEIVECOSTCODENEW"+install).children('option:not(:first)').remove();
		for(var y=0;y<arr_Receive.length;y++){
			if(arr_Receive[y] != ""){
				var cost = arr_Receive[y].split('*');
				if(cost[3] == "384"){//bankid
					$("#PM_C_RECEIVECOSTCODENEW"+install).append("<option value='"+cost[0]+"'>"+cost[1]+"</option>");
				}
			}
		}
	}

	else{
		$("#PM_C_RECEIVECOSTCODENEW"+install).children('option:not(:first)').remove();
	}
}

function selCostcode(inst,txt) {
	var sel = document.getElementById(inst);
	for ( var i=0; i< sel.length; i++ )
	{
	  if ( sel.options[i].value == txt )
	  {
		 sel.selectedIndex = i;
	  }
	}
}

function addCommas(nStr){
		nStr += "";
		x = nStr.split(".");
		x1 = x[0];
		x2 = x.length > 1 ? "." + x[1] : "";
		var rgx = /(\d+)(\d{3})/;
		while (rgx.test(x1)) {
			x1 = x1.replace(rgx, "$1" + "," + "$2");
		}

		return x1 + x2;
}

function returnzero(val){
	if(val=="" || val==undefined){
		val=0;
	}
	return val;
}

function uncommas(value){//ตัดเครื่องหมายคอมม่า(,)
	var res="0";
	if(value){
		res=value.replace(",","");
	}
	return res;
}

function blurnumfloat(obj){
		var lop = 0;
		var val=obj.value;
		if(parseInt(val) < 0) { lop = 1;}
		var val = val.replace("-","");
		if(val!=""){
			var ckvalid=val.split(".");

			var strl=ckvalid[0]; strl=strl.replace(/,/g, "");  strl=strl.split("");
			var lenc=strl.length;
			var str="";
			var m=1;
			for(i=(lenc-1);i>=0;i--){
				 if((m%3)==0 && i>0){str=","+strl[i]+""+str;}
				 else{str=strl[i]+""+str;}
				 m++;
			}
			if(typeof(ckvalid[1])!="undefined"){
				if(ckvalid[1].length == 0){obj.value=(lop == 1 ? "-":"")+str+"."+ckvalid[1]+"00";}
				if(ckvalid[1].length == 1){obj.value=(lop == 1 ? "-":"")+str+"."+ckvalid[1]+"0";}
				if(ckvalid[1].length == 2){obj.value=(lop == 1 ? "-":"")+str+"."+ckvalid[1];}
			}
			else{obj.value=(lop == 1 ? "-":"")+str+"."+"00";}
		}
}
</script>
