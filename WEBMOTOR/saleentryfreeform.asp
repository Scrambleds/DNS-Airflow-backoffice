<%@ LANGUAGE=VBScript %>
<%
Option Explicit
dim rndx
rndx = Randomlink()

Response.Expires = 0		'If you clear the cache, you will lose data in text control
Response.Buffer = True		'Page might be redirectd

If Session("staffid") = ""   Then
   Response.Redirect "login.asp?" &randomlink
End If

If Session("leadid") = ""   Then
   Response.Redirect "crm1.asp?" &randomlink
End If

'-----patt----- buyer from TQM web
If Session("staffid") = "28202"   Then
   Response.Redirect "saleentry_web.asp?leadid=" & Session("leadid")  & "&leadassignid=" & Session("leadassignid") & "&pid=" & request("pid") & "&pmode=" & request("pmode")
End If

dim objConn, objrst
Set objConn = Server.CreateObject("ADODB.Connection")
objConn.Open Application("DSN")

Dim ResLead,DiscountComTeam,DiscountComStaff
Set ResLead = objConn.Execute("SELECT tqmsale.CHECKITEMLIST((select nvl(max(configvalue),'-') from tqmsale.sysconfig s where Config = 'LOCK_EXDISCOUNTCOM'),'"& session("departmentcode") &"') discomteam,tqmsale.CHECKITEMLIST((select nvl(max(configvalue),'-') from tqmsale.sysconfig s where Config = 'UNLOCK_EXDISCOUNTCOM'),'"& session("staffcode") &"') discomstaff FROM TQMSALE.LEAD WHERE LEADID = " & request("leadid"))

DiscountComTeam = ResLead("discomteam")
DiscountComStaff = ResLead("discomstaff")

%>
<style>
.tabcontent {
  display: none;
  padding: 6px 12px;
  border: 1px solid #ccc;
  border-top: none;
}
</style>
<!--#include file=include\header.inc -->
<script language="javascript" src="include/jquery-1.8.3.min.js"></script>
<script language="javascript" src="include/javadate_nohead.js"></script>
<script language="javascript" src="include/javadetail_nohead.js"></script>
<script language="javascript" src="include/saleentry.js?t=<%=Time%>"></script>
<script language="javascript" src="include/calendarDateInput_nohead.js"></script>
<!--#include file=include\calendarDateInput.js -->
<!--#include file=include\menu.inc -->

<SCRIPT LANGUAGE="JavaScript">



<!-- Begin

$(document).ready(function() {
	var cmbnation = document.getElementById("cmbnation");
	var txtNATIONALITY = document.getElementById("NATIONALITY");
	if(cmbnation.value === '1'){
		txtNATIONALITY.value = '';
        txtNATIONALITY.disabled = true;
	}else{
		txtNATIONALITY.val = '';
        txtNATIONALITY.disabled = false;
	}
	$('[name="cmbnation"]').change(function() {
        if ($(this).val() === '1') {
			txtNATIONALITY.value = '';
            txtNATIONALITY.disabled = true;
        }
        else {
            txtNATIONALITY.disabled = false;
        }
    });
	RelationCard();
	
	var cmbrace = document.getElementById("cmbrace");
	var txtRACE = document.getElementById("RACE");
	if(cmbrace.value === '1'){
		txtRACE.value = '';
        txtRACE.disabled = true;
	}else{
		txtRACE.val = '';
        txtRACE.disabled = false;
	}
	$('[name="cmbrace"]').change(function() {
        
        if ($(this).val() === '1') {
			txtRACE.value = '';
            txtRACE.disabled = true;
        }
        else {
			//txtRACE.val = '';
            txtRACE.disabled = false;
        }
    });
	
	$('#chkSameCus').change(function(){
        if(this.checked){
            $('[name="insureprefix"]').val($('[name="leadprefix"]').val());
            $('#insurename').val($('#leadname').val());
            $('#insuresurname').val($('#leadsurname').val());
            $('#citizenid').val($('#customercitizenid').val());
			$('[name="insurebirthdate"]').val($('#birthdate').val());
			$('#insurebirthdate_Day_ID').val($('#birthdate_Day_ID').val());
			$('#insurebirthdate_Month_ID').val($('#birthdate_Month_ID').val());
			$('#insurebirthdate_Year_ID').val($('#birthdate_Year_ID').val());
			//$('[name="insuretype"]').val($('[name=corpstatus]').val());
        }
    });
});	
	
function copyaddfull(src, target) {
	document.getElementById(target +"prefix").value = document.getElementById(src + "prefix").value;
	document.getElementById(target +"no").value = document.getElementById(src + "no").value;
	document.getElementById(target +"bystreet").value = document.getElementById(src + "bystreet").value;
	document.getElementById(target +"street").value = document.getElementById(src + "street").value;
	document.getElementById(target +"area").value = document.getElementById(src + "area").value;
	document.getElementById(target +"building").value = document.getElementById(src + "building").value;
	document.getElementById(target +"path").value = document.getElementById(src + "path").value;
	document.getElementById(target +"districttype").value = document.getElementById(src + "districttype").value;
	document.getElementById(target +"group").value = document.getElementById(src + "group").value;
	document.getElementById(target +"dis").value = document.getElementById(src + "dis").value;
	document.getElementById(target +"province").value = document.getElementById(src + "province").value;
	document.getElementById(target +"zipcode").value = document.getElementById(src + "zipcode").value;
	document.getElementById(target +"contact").value = document.getElementById(src + "contact").value;
	document.getElementById(target +"addphone").value = document.getElementById(src + "addphone").value;
	document.getElementById(target +"routeid").value = document.getElementById(src + "routeid").value;
}


function occother() {

   var Index = document.getElementById("occupationid").selectedIndex;
   if ( document.getElementById("occupationid").options[Index].text == 'อื่นๆ') {
      document.getElementById("jobname").disabled =false;
   }
   else {
      document.getElementById("jobname").value = document.getElementById("occupationid").options[Index].text ;
      //document.getElementById("jobname").disabled =true; 
   }

}


function relaother() {
   var Index = document.getElementById("relationcode").selectedIndex;
   if ( document.getElementById("relationcode").options[Index].text == 'อื่นๆ') {
      document.getElementById("relationtext").disabled =false; 
   }
   else {
      document.getElementById("relationtext").value = document.getElementById("relationcode").options[Index].text ;
      //document.getElementById("relationtext").disabled =true; 
   }
}

function relaotherext(id) {

   var Index = document.getElementById("relationcode"+id).selectedIndex;
   if ( document.getElementById("relationcode"+id).options[Index].text == 'อื่นๆ') {
      document.getElementById("relationtext"+id).disabled =false; 
   }
   else {
      document.getElementById("relationtext"+id).value = document.getElementById("relationcode"+id).options[Index].text ;
      //document.getElementById("relationtext"+id).disabled =true; 
   }

}

function RelationCard(){
	var cardholderrelation = document.getElementById("cardholderrelation").value;
	var cardholderrelation_txt = document.getElementById("cardholderrelation_txt").value;
	if(cardholderrelation == "อื่นๆ"){
		document.getElementById("cardholderrelation_txt").disabled = false;
		if(cardholderrelation_txt != ""){
			document.getElementById("cardholderrelation_txt").value = cardholderrelation_txt;
		}else{
			document.getElementById("cardholderrelation_txt").value = "";
		}		
	}else{
		document.getElementById("cardholderrelation_txt").disabled = true;
		document.getElementById("cardholderrelation_txt").value = "";
	}
}


function getquestion(str)
{

	var url="questionext.asp?saleid=" + str;
	window.open(url,"POSTQUESTION","width=525,height=80,top=200,left=100,resizable=no,scrollbars=no");

}

function getcredit(vsaleid)
{
	//alert(vsaleid);
	var url="postcredit.asp?saleid=" + vsaleid ;
	window.open(url,"GETCREDIT","width=560,height=240,top=200,left=100,resizable=yes,scrollbars=yes");

}

function getpost(src)
{

	var Index = document.getElementById(src+"province").selectedIndex;
	var url="postcode.asp?src=" + src + "&province=" +document.getElementById(src+"province").options[Index].text ;
		url = url +  "&dis=" +document.getElementById(src+"dis").value;
		try {
			url = url +  "&area=" +document.getElementById(src+"add2").value;
			}
			catch(err)
			{
			url = url +  "&add2=" +document.getElementById(src+"area").value;
			}
	window.open(url,"POSTCODE","width=560,height=240,top=200,left=100,resizable=yes,scrollbars=yes");

}

function getExt(src)
{

	var url="getsalelist.asp?src=" + src + "";
	window.open(url,"EXTENDSALE","width=560,height=240,top=200,left=100,resizable=yes,scrollbars=yes");

}

function getroute(src)
{
    var url="routesearch.asp?src=" + src;
    window.open(url,"ROUTECODE","width=720,height=500,top=100,left=100,resizable=yes,scrollbars=yes");
}

function getroute1()
{
	var url="routesearch.asp?";
	window.open(url,"ROUTECODE","width=720,height=500,top=100,left=100,resizable=yes,scrollbars=yes");

}

$(document).ready(function(){
		if(<%="'" & request("suppliercode") & "'"%>){
			selectsupplier();
		}
	});
	
	
function AllowNumeric(evt)
{
    var charcode;
    charcode = (evt.which) ? evt.which : event.keyCode;
    if ((charcode >= 48 && charcode <= 57) || charcode == 72 || charcode == 79)
    {
        if ($(evt).val().length >= 5)
        { return false; }
        else
        { return true; }
    }
    else
    { return false; }
}


function addOption(selectbox, value, text , color)
{
	var optn = document.createElement("OPTION");
	optn.text = text;
	optn.value = value;
	//if (color=1) { optn.style.color='blue'; }
	selectbox.options.add(optn);
}

function removeAllOptions(selectbox)
{
	var i;
	for(i=selectbox.options.length-1;i>=0;i--)
	{
		selectbox.remove(i);
	}
}

function setrouteid(src)
{
    //alert(src + document.getElementById(src+"routeid").value);
	if (document.getElementById(src+"routeid").value != '' )
	{
		document.getElementById("routeid").value = document.getElementById(src+"routeid").value;
	}

}

function COST(install,type){
    // ChkInstallOneKeyin();
	// type : K = Keyin , R = Credit Card
	var SaleType = $("#SALETYPE").val();
	var installrun = document.getElementById("installment").value; // จำนวนงวด
	var CorpType = $("#corpstatus").val();
	var FlagSalaryMan = "N";
	var strCostname;
	var count;
	if (SaleType == "N" || SaleType== "I" || SaleType== "C" || SaleType== "R") {
		FlagSalaryMan = "Y";
	}else{
		FlagSalaryMan = "N"; 
	}
	var countst = 1;
	$("#TOTALLOAN").hide(); 
	var bank = document.getElementById("cardbankid").value;	
	if(type == "0"){
        //var bank = document.myform.cardbankid.value ;
		var str = <%="'" & application("strRECEIVECOST") & "'" %>;
		var arr = str.split('|');
		for(j=1;j<=installrun;j++){
		    var sel = document.getElementById("RECEIVECOST"+j);
		    var sel1 = document.getElementById("INSTALLTYPE"+j);
		    removeAllOptions(sel);
			addOption(sel,'', '** กรุณาเลือก cost code **',0);
			if (sel1.value == "R" || sel1.value == "K"){
				for(var i=0;i<arr.length;i++){
					if (arr[i] != ''){
						var cost = arr[i].split('*');						
						if (cost[3] == bank){ // BankID
							strCostname = cost[1];
							if (FlagSalaryMan == 'Y'){
								if (strCostname.indexOf('PIA Non-Motor') < 0){
									addOption(sel, cost[0], cost[1],0); 
								}								
							}else{ 	
								count = strCostname.indexOf('Salary Man'); 
								if (count < 0  && strCostname.indexOf('PIA Non-Motor') < 0)
								  addOption(sel, cost[0], cost[1],0); 
							}
						}
					}
				}
			}
			if(sel1.value == "A"){
				if(installrun == "2"){
					document.getElementById("TOTALLOAN").style.display = '';
					document.getElementById("TOTALLOAN").style.color = "red";
					$("#INSTALLTYPE2").find("[value='A']").removeAttr("selected").attr("disabled",true);
				}				
			}
		}
	}else if (type == "R" || type == "K"){
		//var bank = document.myform.cardbankid.value ;
		var str = <%="'" & application("strRECEIVECOST") & "'" %>; 
		var arr = str.split('|');
        var sel = document.getElementById("RECEIVECOST"+install);
		removeAllOptions(sel);
		addOption(sel,'', '** กรุณาเลือก cost code **',0);

		for(var i=0;i<arr.length;i++){		
			if (arr[i] != ''){
				var cost = arr[i].split('*');
				if (cost[3] == ''){ // BankID				
					if (cost[0] == 'TQMFULL'){
						addOption(sel, cost[0], cost[1],0);
						countst=countst+1;
					}else if (cost[0] == 'TQMFULL01'){
						addOption(sel, cost[0], cost[1],0);
						countst=countst+1;
					}
				}else{
					if (cost[3] == bank){
						strCostname = cost[1];
						if(FlagSalaryMan == 'Y'){
							if (strCostname.indexOf('PIA Non-Motor') < 0){
								addOption(sel, cost[0], cost[1],0); 
							}
						}else{
							count = strCostname.indexOf('Salary Man');
							if (count < 0 && strCostname.indexOf('PIA Non-Motor') < 0){
								addOption(sel, cost[0], cost[1],0); 								
							}
						}
					}
				}
			}
		}
	}

	else if (type == "G"){
        var str = '<%=application("strRECEIVECOST")%>'; 
        var arr = str.split('|');
        var sel = document.getElementById("RECEIVECOST"+install);
        removeAllOptions(sel);
        addOption(sel,'', '** กรุณาเลือก cost code **',0);

        for(var i=0;i<arr.length;i++){		
            if (arr[i] != ''){
                var cost = arr[i].split('*');
                // แสดงเฉพาะรายการที่มี BANKID = 384
                if (cost[3] == '384'){ 
                    strCostname = cost[1];
                    if(FlagSalaryMan == 'Y'){
                        if (strCostname.indexOf('PIA Non-Motor') < 0){
                            addOption(sel, cost[0], cost[1],0); 
                        }
                    }else{
                        count = strCostname.indexOf('Salary Man');
                        if (count < 0 && strCostname.indexOf('PIA Non-Motor') < 0){
                            addOption(sel, cost[0], cost[1],0); 								
                        }
                    }
                }
            }
        }
    }
	
	else{
		var sel = document.getElementById("RECEIVECOST"+install);
		removeAllOptions(sel);
		addOption(sel,'', '** กรุณาเลือก cost code **',0);
	}
}


function selCOST(inst,txt) {
 var sel = document.getElementById("RECEIVECOST"+inst);
		for ( var i=0; i< sel.length; i++ )
		{
		  if ( sel.options[i].value == txt )
		  {
			 sel.selectedIndex = i;
		  }
		}
}

function selectsupplier(){
	var supplier =  document.getElementById("suppliercode");
	var product =  document.getElementById("productid_sel");
	var companyid = '<%=session("companyid")%>';
	
	removeAllOptions(product);
	addOption(product,'', 'เลือกMODE สินค้า'); 
	var strSup = <%="'" & application("cmbSUPPLIERNMT") & "'" %> ;
	var strSup = strSup.split("|");
	var strSups = "";
	for(x=0; x<strSup.length; x++){
		strSups = strSup[x].split("*");
		if(supplier.value == strSups[0] && supplier.value){
			if(companyid == strSups[4]){
				addOption(product,strSups[1],strSups[2]+":"+strSups[3],0);
			}
		}
	}
	var chkproduct = "<% Response.Write request("productid")%>";
	for(x=0; x<product.length; x++){
		if(product[x].value == chkproduct){ 
			product.selectedIndex = x;
			document.getElementById("productid").value = chkproduct;
		}
	}
}


	$(function(){
		/*$(".inputsalevalue").change(function(){
			check_number_cal($(this).attr('id'));	
		});	*/
	})
	
	function check_number_cal(objIDActive){
		var conf_dutyrate = 0.004;
		var conf_vatrate = 0.07;
		var amount = parseFloat(document.getElementById("amount").value);
		if (!amount){
			document.getElementById("amount").value = 0;
			var amount =  parseFloat(document.getElementById("amount").value);
		}
		var duty = parseFloat(document.getElementById("duty").value);
		if (!duty){
			document.getElementById("duty").value = 0;
			var duty =  parseFloat(document.getElementById("duty").value);
		}
		var vat = parseFloat(document.getElementById("vat").value);
		if (!vat){
			document.getElementById("vat").value = 0;
			var vat =  parseFloat(document.getElementById("vat").value);
		}
		var extdiscom_percent = parseFloat(document.getElementById("extdiscom_percent").value);
		var discountother_percent = parseFloat(document.getElementById("discountother_percent").value);
		var balance = parseFloat(document.getElementById("balance").value);
		var taxrate = parseFloat(document.getElementById("txttaxrate").value);
		var extdissale = parseFloat(document.getElementById("extdissale").value);
		var extdiscom = parseFloat(document.getElementById("extdiscom").value);
		var netvalue = parseFloat(document.getElementById("netvalue").value);
		var netamount = parseFloat(document.getElementById("netamount").value);
		var discountother = 0;
		var extdiscom = 0;
		var taxvalue = 0;
		if (objIDActive == 'amount'){	
			if (amount == ""){
				amount = 0;
			}
			duty = Math.ceil(amount * conf_dutyrate);
			vat = (amount + duty ) * conf_vatrate;
			netvalue = amount + duty + vat;
			document.getElementById("duty").value = Math.ceil(duty).toFixed(2);
			document.getElementById("vat").value = vat.toFixed(2);
			document.getElementById("netvalue").value = parseFloat(netvalue).toFixed(2);
			
			if (extdiscom_percent){
				extdiscom = (extdiscom_percent*netvalue)/100;
				document.getElementById("extdiscom").value = extdiscom.toFixed(2);
			}
			
			if (discountother_percent){
				discountother = (extdiscom_percent*netvalue)/100;
				document.getElementById("discountother").value = discountother.toFixed(2);
			}

			netamount = netvalue - (extdissale + extdiscom + discountother);
			document.getElementById("netamount").value = parseFloat(netamount).toFixed(2);
			document.getElementById("totalamount").value = parseFloat(netamount).toFixed(2);
			taxvalue = ((amount + duty)*taxrate)/100;
		}else if (objIDActive == 'duty'){
			if (duty == ""){
				duty = 0;
			}
			netvalue =  amount + duty + vat;
			document.getElementById("netvalue").value =  parseFloat(netvalue).toFixed(2);
			
			if (extdiscom_percent){
				extdiscom = (extdiscom_percent*netvalue)/100;
				document.getElementById("extdiscom").value = extdiscom.toFixed(2);
			}
			
			if (discountother_percent){
				discountother = (extdiscom_percent*netvalue)/100;
				document.getElementById("discountother").value = discountother.toFixed(2);
			}
			netamount = netvalue - (extdissale + extdiscom + discountother);
			document.getElementById("netamount").value = parseFloat(netamount).toFixed(2);
			document.getElementById("totalamount").value = parseFloat(netamount).toFixed(2);
			taxvalue = ((amount + duty)*taxrate)/100;
		}else if (objIDActive == 'vat'){
			if (vat == ""){
				vat = 0;
			}
			netvalue =  amount + duty + vat;
			document.getElementById("netvalue").value =  parseFloat(netvalue).toFixed(2);
			
			if (extdiscom_percent){
				extdiscom = (extdiscom_percent*netvalue)/100;
				document.getElementById("extdiscom").value = parseFloat(extdiscom).toFixed(2);
			}
			
			if (discountother_percent){
				discountother = (extdiscom_percent*netvalue)/100;
				document.getElementById("discountother").value = parseFloat(discountother).toFixed(2);
			}
			netamount = netvalue - (extdissale + extdiscom + discountother);
			document.getElementById("netamount").value = parseFloat(netamount).toFixed(2);
			document.getElementById("totalamount").value = parseFloat(netamount).toFixed(2);
			taxvalue = ((amount + duty)*taxrate)/100;
		}else if (objIDActive == 'extdissale'){
			if (extdissale == ""){
				extdissale = 0;
			}
			
			netvalue =  amount + duty + vat;
			document.getElementById("netvalue").value =  parseFloat(netvalue).toFixed(2);
			
			if (extdiscom_percent){
				extdiscom = (extdiscom_percent*netvalue)/100;
				document.getElementById("extdiscom").value = parseFloat(extdiscom).toFixed(2);
			}
			
			if (discountother_percent){
				discountother = (extdiscom_percent*netvalue)/100;
				document.getElementById("discountother").value = parseFloat(discountother).toFixed(2);
			}
			netamount = netvalue - (extdissale + extdiscom + discountother);
			document.getElementById("netamount").value = parseFloat(netamount).toFixed(2);
			document.getElementById("totalamount").value = parseFloat(netamount).toFixed(2);
			taxvalue = ((amount + duty)*taxrate)/100;
		}else if (objIDActive == 'extdiscom'){
			if (extdiscom == ""){
				extdiscom = 0;
			}
			
			netvalue =  amount + duty + vat;
			document.getElementById("netvalue").value =  parseFloat(netvalue).toFixed(2);
			
			if (discountother_percent){
				discountother = (extdiscom_percent*netvalue)/100;
				document.getElementById("discountother").value = parseFloat(discountother).toFixed(2);
			}
			netamount = netvalue - (extdissale + extdiscom + discountother);
			document.getElementById("netamount").value = parseFloat(netamount).toFixed(2);
			document.getElementById("totalamount").value = parseFloat(netamount).toFixed(2);
			taxvalue = ((amount + duty)*taxrate)/100;
		}else if (objIDActive == 'extdiscom_percent'){
			if (extdiscom_percent == ""){
				extdiscom_percent = 0;
			}
			
			netvalue =  amount + duty + vat;
			document.getElementById("netvalue").value =  parseFloat(netvalue).toFixed(2);
			extdiscom = (extdiscom_percent*netvalue)/100;
			
			if (discountother_percent){
				discountother = (extdiscom_percent*netvalue)/100;
				document.getElementById("discountother").value = parseFloat(discountother).toFixed(2);
			}
			netamount = netvalue - (extdissale + extdiscom + discountother);
			document.getElementById("netamount").value = parseFloat(netamount).toFixed(2);
			document.getElementById("totalamount").value = parseFloat(netamount).toFixed(2);
			taxvalue = ((amount + duty)*taxrate)/100;
		}else if (objIDActive == 'discountother_percent'){
			if (discountother_percent == ""){
				discountother_percent = 0;
			}			
			netvalue =  amount + duty + vat;
			document.getElementById("netvalue").value =  parseFloat(netvalue).toFixed(2);			
			if (discountother_percent){
				discountother = (extdiscom_percent*netvalue)/100;
				document.getElementById("discountother").value = parseFloat(discountother).toFixed(2);
			}
			netamount = netvalue - (extdissale + extdiscom + discountother);
			document.getElementById("netamount").value = parseFloat(netamount).toFixed(2);
			document.getElementById("totalamount").value = parseFloat(netamount).toFixed(2);
			taxvalue = ((amount + duty)*taxrate)/100;
		}else if (objIDActive == 'taxrate'){
			if (taxrate == ""){
				taxrate = 0;
			}			
			netvalue =  amount + duty + vat;
			document.getElementById("netvalue").value =  parseFloat(netvalue).toFixed(2);			
			netamount = netvalue - (extdissale + extdiscom + discountother);
			document.getElementById("netamount").value = parseFloat(netamount).toFixed(2);
			document.getElementById("totalamount").value = parseFloat(netamount).toFixed(2);
			taxvalue = ((amount + duty)*taxrate)/100;
			document.getElementById("taxvalue").value = parseFloat(taxvalue).toFixed(2);
		}		
	}
	
</script>

<%
	Dim i,x
	dim productname, netamount, payschedule
	dim rslead, addtemp, addZip, addLine1, addLine2, addDistrict, addProvince
	dim tmpstr1, tmpstr2, tmpstr3
	dim paymentCR, paymentDR,paymentDC,  saleaction
	dim submiterror
	dim policydate
	dim readmode
	dim deliverydate, showquestion
	dim nomineecount, producttype, getcustomer,	getcustomernominee,	getnominee
	dim bdate, insuredate, mapprefix
	dim cnt, premiumamount, newamount, prefix_eng, supplierid
	dim addresssplit
	dim  installment,totalAmount, RunAmount, setamount
	dim countplateid
	
	if request("read") = "1" then
	   readmode =1
	   session("leadid") = request("leadid")
	   session("leadassignid") = request("leadassignid")
	else
	   readmode =0
	end if
	countplateid = request("countplateid")
	if not isnull(countplateid) then
		countplateid =cnum(request("countplateid"))
	end if 
	
	installment = request("installment")
	if installment = "" then
	   installment =1
	else
	   installment =cnum(request("installment"))
	end if
	newamount = request("newamount")

    policydate = request("policydate")
    if policydate = "" then
       policydate =  getPolicydate ' formatDateEdit(date()+1)
    end if

	deliverydate = request("deliverydate")

    if deliverydate = "" then
       deliverydate =  getPolicydate
    end If

	policydate = datefix(policydate)
	deliverydate = datefix(deliverydate)

	call getLeadrs
	'call getProductInfo

	if request("cmd") ="1" then
	   call cmdVerify
    end if
	if request("cmd") ="2" then
	   call cmdSave
	   if isnull(submiterror) then
	      response.redirect "saleok.asp"
	   end if
    end If


 	tmpstr1 =""
	if request("policydate") <> "" and getExpirydateErr(todate(request("policydate")), todate(request("expirydate")))  = 1 then
	   tmpstr1 = tmpstr1 & " อายุ กธ ไม่ใช่ 1 ปี "
	end if

	if tmpstr1 <> "" then
	   response.write "<font style='font: 12pt Verdana; color:#F00000;'>*** " & tmpstr1 & " ***</font>"
	end if

	bdate = getvalue("birthdate")
	if isnull(bdate) or bdate ="" then
		bdate = datefix(date())
	else
		bdate = datefix(bdate)
	end if
	
	insuredate = getvalue("birthdate")
	if isnull(insuredate) or insuredate ="" then
		insuredate = datefix(date())
	else
		insuredate = datefix(insuredate)
	end if
	
	call printSaleSession
	mapPrefix =getleaddata("leadprefix")
	if mapPrefix ="น.ส." then mapPrefix="นางสาว"
	
	dim id 
	id = getleaddata("citizenid")
	Response.write "<form name=myform METHOD=POST  autocomplete=off >"
	if not isnull(submiterror) and submiterror <> "" then
	   Response.write "<table width='100%'><tr><td height=30  align=center bgcolor=#F0F0F0><div style='font: 11pt tahoma; color:#CC0000; '>"
	   Response.write submiterror
	   Response.write "</td></tr></table>"
	end If
			   
	Response.write "<table width='100%'><tr><td class=HAlert>&nbsp;<img src=images/bullet2.gif> ข้อมูลลูกค้า  " & _	
				   "</td><td align=right>" & _
				   "</tr></table>"
					
	Response.write "<input type=hidden id=SOURCE name=SOURCE value='TSR-FREEFORM'>"
	
	Response.write "<table>" & _ 
				   "<tr><td width=10></td> " & _
	 			   "<td width=50>ชื่อ-สกุล </td>"  & _
				   "<td width=50 >" &  getComboTXT("leadprefix", "cmbprefix",mapPrefix)  & "</td> "  & _
	 			   "<td>" & inputTag("leadname","T","",20) & "</td>" & _
				   "<td>" & inputTag("leadsurname","T","",20) & "</td>" & _
				   "<td>ในนาม " &  getCombo("corpstatus", "cmbCorpStatus","") & "</td>" & _	
 			       "</tr></table>"

    Response.write "<table><tr><td width=10></td> " & _
				   "<td  width=50>ประเภท </td>" & _
				   "<td>" &  getCombo("SALETYPE", "cmbSaleTypeNonmotor","N")  & "</td> "  & _  
				   "<td width=50>เลขที่บัตร</td>"  & _
				   "<td> " & inputTag("customercitizenid","T",id,15) 
					response.write " <img src='images/noidcard.png' width='15px' height='15px' onclick=""getCitizenDummy('customercitizenid')"" style='cursor:pointer;' title='Auto เลขบัตรประชาชน Dummy' />" & _ 
				   "<td>ความสำคัญ " &  getCombo("salepiority", "cmbSalePiority","")  & "</td>" & _ 					
					"<td><a href='javascript:getCloneSaleid();'><img src='images/copy.jpg' title='คัดลอกข้อมูลของรายการขายที่ต้องการแทน'  border='0' /></a> </td>" & _ 
 			       "</tr></table>"
				   
	Response.write "<table><tr><td width=10></td> " & _
 				    "<td width=50 >เชื้อชาติ </td><td>" & getComboNation("cmbrace",request("cmbrace")) & inputTag("RACE","T","",10) & "</td>"  & _
 				    "<td >สัญชาติ </td><td>" & getComboNation("cmbnation",request("cmbnation")) & inputTag("NATIONALITY","T","",10) & "</td>"  & _
 			        "</tr></table>"

	Response.write "<table><tr><td width=10></td>" & _
				   "<td width=50 >เกิดวันที่ </td>"& _
				   "<td><script>DateInput('birthdate', true, 'DD/MM/YYYY', '" &  bdate & "')</script></td>" & _
				   "<td >เพศ " &  getCombo("gender", "cmbgender","M") & "</td>" & _
				   "<td>สถานะภาพ " &  getCombo("martialstatus", "cmbmartialstatus","S")  & "</td>" & _
				   "<td >บุตร " &  inputTagMax("child","N","0",3,2) & "</td>" & _
 			       "</tr></table>"
	Response.write "<table><tr><td width=10></td>" & _
				   "<td width=50 >โทรมือถือ</td><td> " & inputTag("phonemobile","T",p1,32) & "</td>" & _
				   "<td >โทรบ้าน</td><td>" & inputTag("phonehome","T",p2,32) & "</td>" & _
				   "<td >โทรที่ทำงาน </td><td>" & inputTag("phoneoffice","T",p3,32) & "</td>" & _
				   "</tr>"
	
	response.write "<input type=hidden id=leadphonemobile name=leadphonemobile value='" & request("leadphonemobile") & "'>"
	response.write "<input type=hidden id=leadphonehome name=leadphonehome value='" & request("leadphonehome") & "'>"
	response.write "<input type=hidden id=leadphoneoffice name=leadphoneoffice value='" & request("leadphoneoffice") & "'>"

	response.write "<input type=hidden id=leadphonemobile_hide name=leadphonemobile_hide value='" & request("leadphonemobile_hide") & "'>"
	response.write "<input type=hidden id=leadphonehome_hide name=leadphonehome_hide value='" & request("leadphonehome_hide") & "'>"
	response.write "<input type=hidden id=leadphoneoffice_hide name=leadphoneoffice_hide value='" & request("leadphoneoffice_hide") & "'>"

	Response.write "<tr><td width=10></td>" & _
				   "<td width=50 align=right><a href=getphonelist.asp?source=leadphonemobile&rnd=" & rndx & " onclick='return show_hide_box(this, 230, 400, ""1px solid #C0A0A0"");' id=mobile><img src=images/phonetake.jpg  border=0 ></a></td>" & _
				   "<td align=left id=leadphonemobile_show style='color:#0000A0;'>" &  request("leadphonemobile_hide") & "</td>" & _
				   "<td align=right><a href=getphonelist.asp?source=leadphonehome&rnd=" & rndx & " onclick='return show_hide_box(this, 230, 400, ""1px solid #C0A0A0"");'  id=home><img src=images/phonetake.jpg  border=0 ></a></td>" & _
				   "<td id=leadphonehome_show style='color:#0000A0;'>" &  request("leadphonehome_hide") & "</td>" & _
				   "<td align=right><a href=getphonelist.asp?source=leadphoneoffice&rnd=" & rndx & " onclick='return show_hide_box(this, 230, 400, ""1px solid #C0A0A0"");'  id=office><img src=images/phonetake.jpg  border=0></a></td>" & _
				   "<td id=leadphoneoffice_show style='color:#0000A0;'>" &  request("leadphoneoffice_hide") & "</td>" & _
				   "<td></td>" & _
				   "<td></td>" & _
 			       "</tr></table>"

	Response.write "<table><tr><td width=10></td>" & _
				   "<td width=50 >FAX</td>" & _
				   "<td >" & inputTag("phonefax","T","",30) & "</td>" & _
				   "<td ><font color=#0000ff>EMAIL &nbsp;" & replace(inputTag("email","T","",20),"<input " ,"<input onchange='validateEmail(this)'") & "<b> (โครงการ TQM Digital กรุณาระบุ EMAIL)</b></font></td>"  & _
				   "</tr></table>"

    Response.write "<table><tr><td width=10></td>" & _
	  			   "<td width = 100 >บริษัท</td>" &  _
				   "<td>" & inputTag("company","T","",30) & "</td>" & _
				   "<td>ตำแหน่ง</td><td>" & inputTag("jobtitle","T","",20) & "</td>" & _
				   "<td>รายได้ต่อปี</td><td>" & inputTag("income","N","",20) & "</td>" & _
				   "</tr>" & _
				   "<tr><td width=10></td>" & _
	  			   "<td >ความพิการ/โรคอื่นๆ  </td>" &  _
				   "<td>" & inputTag("handicap","T","",30)  & "</td>" & _
				   "<td>รหัสอาชีพ </td><td>" & replace(getCombo("occupationid", "cmboccupation",matchstrcmb("cmboccupation",getleaddata("jobname"))),"<select ","<select onchange='occother()'  ")  & "</td>" & _
				   "<td width=56>อาชีพ</td><td>" &  inputTag("jobname","T","",20) & "</td>" & _
				   "</tr></table>"
				   
	Response.write "<table><tr><td width=10></td>" & _
	  			   "<td width = 100 >พนักงานแนะนำ LEAD</td>" &  _
				   "<td>" & getCombo("refstaffcode", "cmbStaffRefer",request("refstaffcode")) & "</td>" & _
				   "<td></td>" & _
				   "<td></td>" & _
				   "</tr>" & _
				   "<tr><td width=10></td>" & _
	  			   "<td></td>" &  _
				   "<td></td>" & _
				   "<td></td>" & _
				   "<td width=56></td><td></td>" & _
				   "</tr></table>"


	
	'----- แยกที่อยู่ ----- set productcig >>> GETADDRESSSPLIT="Y"
	Response.write "<table><tr><td width = 800 height=2 bgcolor =#E0E0E0></td></tr></table>"
	Response.write "<div class=HAlert>&nbsp;<img src=images/bullet2.gif> ที่อยู่</div>"
	Response.write "<table>"
	Response.write "<tr valign=top bgcolor=#f7f7f7 valign=top><td width=10 bgcolor=white ></td>"
	' Modify By Thanakorn.Pak DataCleansing BR-5680 14/07/2020 START
		Response.write "<td width = 250 valign=bottom><b><font color=#0000A0>ทะเบียนบ้าน</b></font> &nbsp;&nbsp; COPY : <a href='javascript:copyaddfull(""off"",""home"");'  ><b>:ที่ทำงาน</a>  <a href='javascript:copyaddfull(""contact"",""home"");'> <b>:ที่ติดต่อ</a></td>"
		Response.write "<td width = 250 valign=bottom><b><font color=#0000A0>ที่ทำงาน</b></font> &nbsp;&nbsp; COPY : <a href='javascript:copyaddfull(""home"",""off"");'  ><b>:ทะเบียน</a>  <a href='javascript:copyaddfull(""contact"",""off"");'> <b>:ที่ติดต่อ</a></td>"
		Response.write "<td width = 250 valign=bottom><b><font color=#0000A0>ที่ติดต่อ</b></font> &nbsp;&nbsp; COPY : <a href='javascript:copyaddfull(""home"",""contact"");'  ><b>:ทะเบียน</a>  <a href='javascript:copyaddfull(""off"",""contact"");'> <b>:ที่ทำงาน</a></td>"
	' Modify By Thanakorn.Pak DataCleansing BR-5680 14/07/2020 END
	Response.write "</tr>"

	tmpstr1 = request("contactaddresstype")
	tmpstr2 = request("policyaddresstype")
	tmpstr3 = request("customeraddresstype")

	Response.write "<tr valign=top><td width=10></td>"
	Response.write "<td width = 250  bgcolor=#E7E7E7>"
	Response.write "<input type=hidden name=homeroute  value=" & request("homeroute")  & ">"
	Response.write "<input type=hidden name=offroute  value=" & request("offroute")  & ">"
	Response.write "<input type=hidden name=contactroute  value=" & request("contactroute")  & ">"

	if request("cmd") ="" then

		addtemp = getleaddata("homeaddress")
		if len(addtemp) <5 then
			   addtemp = getleaddata("contactaddress")
		end if
		call buildaddress
	end if
	
	dim p1 ,p2 ,p3
	dim tmpstr4 , tmpstr5
	tmpstr1 = request("contactaddresstype")
	tmpstr2 = request("policyaddresstype")
	tmpstr3 = request("takephotoaddresstype")
	tmpstr4 = request("taxaddresstype")
	tmpstr5 = request("postaddresstype")
	
	p1=getleaddata("phonemobile")
	p2=getleaddata("phonehome")
	p3=getleaddata("phoneoffice")
	 
	addtemp = getleaddata("homeaddress")
	if request("cmd") ="" then
		if len(addtemp) <5 then
		   addtemp = getleaddata("contactaddress")
		end if
		call buildaddress
	end if

	Response.write addtemp & "<br>"
	Response.write "<table>" & _
				   "<tr valign=top><td>ชื่อ</td><td colspan=3>" & getComboTXT("homeprefix", "cmbprefix",mapPrefix) &  "<br>" & inputTagmax("homename","T",getleaddata("leadname"),10,100) &  inputTagmax("homesurname","T",getleaddata("leadsurname"),11,100)  & "</td></tr>" & _
				   "<tr><td>เลขที่ </td><td>" & inputTagmax("homeno","T",addline1,15,100) & "</td><td align=right>หมู่ </td><td> " & inputTagMax("homegroup","N","",2,50) & "</td></tr>" & _
				   "<tr><td>หมู่บ้าน/ตึก </td><td>" & inputTagmax("homebuilding","T","",15,100) & "</td><td align=right>ซอย</td><td>" & inputTagMax("homebystreet","T","",10,100) & "</td></tr>" & _
				   "<tr><td>ตรอก </td><td>" & inputTagMax("homepath","T","",15,100) & "</td><td align=right>ถนน</td><td>" & inputTagMax("homestreet","T","",10,255) & "</td></tr>" & _
				   "<tr><td>จังหวัด</td><td>" & getCombo("homeprovince", "cmbProvince",addprovince) & "</td><td>" & getCombo("homedistricttype", "cmbdistricttype","A") & "</td><td>" & inputTagmax("homedis","T",addDistrict,10,255) & "</td></tr>" & _
				   "<tr><td>ตำบล/แขวง</td><td colspan=3>" & inputTagMax("homearea","T",addline2,15,255) &  " ป." & inputTagmax("homezipcode","T",addzip,5,5) &   "&nbsp;<a href='javascript:getpost(""home"");'><img src=images/search.gif border=0></a> </td></tr>" & _
				   "<tr><td>ติดต่อ</td><td colspan=3>" & inputTagmax("homecontact","T","",35,100) & "</td></tr>" & _
				   "<tr><td>โทรศัพท์</td><td colspan=3>" & inputTagmax("homeaddphone","T",p2,35,255) & " <input type=hidden id=homeroute name=homeroute value=" & request("homeroute")  & "></td></tr>" & _
				   "</table>" & _
				   "<table>" & _
				   "<tr><td></td><td><input type=radio id=contactaddresstype name=contactaddresstype value=H " & getchecked("H",tmpstr1) & " onclick=""setrouteid('home');"" > ติดต่อ </td></tr>" & _
				   "<tr><td></td><td><input type=radio id=postaddresstype name=postaddresstype value=H " & getchecked("H",tmpstr5) & " > ไปรษณีย์</td></tr>" & _
				   "<tr><td></td><td><input type=radio id=policyaddresstype name=policyaddresstype value=H " & getchecked("H",tmpstr2) & " > แสดงในกรมธรรม์</td></tr>" & _
				   "<tr><td></td><td><input type=radio id=taxaddresstype name=taxaddresstype value=H " & getchecked("H",tmpstr4) & " > พิมพ์ใบกำกับ</td></tr>" & _
				   "<tr><td></td><td>เส้น<a href='javascript:getroute(""home"");' ><img src=images/search.gif border=0></a>" & replace(getCombo("homerouteid", "cmbRoute",""),"430px","200px") & "</td></tr>" & _
				   "</table>"
				   
	Response.write "<table>" & _
				   "<tr><td></td><td></td></tr>" & _
			       "<tr><td>หักภาษี %</td><td>" & replace(inputTagMax("taxrate","N",0,2,3),"<input","<input onChange=Copytaxrate(this.value) ") & _
				   "<input type='radio' name='flgtax' id='taxagree' value='1' "& getchecked(request("flgtax"),"1") &" onChange=""gettaxrate('1')"" />ยินยอม" & _
				   "<input type='radio' name='flgtax' id='taxnonagree' value='0' "& getchecked(request("flgtax"),"0") &" onChange=""gettaxrate('0')"" />ไม่ยินยอม" & _
				   "</td></tr>" & _
			       "</table>"
				   
	Response.write "</td>"

	Response.write "<td width = 250  bgcolor=#E7E7E7>"

	addtemp = getleaddata("officeaddress")
	if request("cmd") ="" then
		call buildaddress
	end if

	Response.write addtemp & "<br>"
	Response.write "<table>" & _
				   "<tr valign=top><td>ชื่อ</td><td colspan=3>" & getComboTXT("offprefix", "cmbprefix",mapPrefix)  & "<br>" &  inputTagmax("offname","T",getleaddata("leadname"),10,100) & inputTagmax("offsurname","T",getleaddata("leadsurname"),12,100)  & "</td></tr>" & _
				   "<tr><td>เลขที่ </td><td>" & inputTagmax("offno","T",addline1,15,100) & "</td><td align=right>หมู่</td><td> " & inputTagMax("offgroup","N","",2,50) & "</td></tr>" & _
				   "<tr><td>หมู่บ้าน/ตึก </td><td>" & inputTagmax("offbuilding","T","",15,100) & "</td><td align=right>ซอย</td><td>" & inputTagMax("offbystreet","T","",10,100) & "</td></tr>" & _
				   "<tr><td>ตรอก </td><td>" & inputTagMax("offpath","T","",15,100) & "</td><td align=right>ถนน</td><td>" & inputTagMax("offstreet","T","",10,255) & "</td></tr>" & _
				   "<tr><td>จังหวัด</td><td>" & getCombo("offprovince", "cmbProvince",addprovince) & "</td><td>" & getCombo("offdistricttype", "cmbdistricttype","A") & "</td><td>" & inputTagMax("offdis","T",addDistrict,10,255) & "</td></tr>" & _
				   "<tr><td>ตำบล/แขวง</td><td colspan=3>" & inputTagMax("offarea","T",addline2,15,255) &  " ป." & inputTagmax("offzipcode","T",addzip,5,5) & "&nbsp;<a href='javascript:getpost(""off"");' ><img src=images/search.gif border=0></a></td></tr>" & _
				   "<tr><td>ติดต่อ</td><td colspan=3>" & inputTagmax("offcontact","T","",35,100) & "</td></tr>" & _
				   "<tr><td>โทรศัพท์</td><td colspan=3>" & inputTagmax("offaddphone","T",p3,35,255) & " <input type=hidden id=offroute name=offroute value=" & request("offroute")  & "></td></tr>" & _
				   "</table>" & _

				   "<table>" & _
				   "<tr><td></td><td><input type=radio id=contactaddresstype name=contactaddresstype value=O " & getchecked("O",tmpstr1) & " onclick=""setrouteid('off');"" > ติดต่อ </td></tr>" & _
				   "<tr><td></td><td><input type=radio id=postaddresstype name=postaddresstype value=O " & getchecked("O",tmpstr5) & " > ไปรษณีย์</td></tr>" & _
				   "<tr><td></td><td><input type=radio id=policyaddresstype name=policyaddresstype value=O " & getchecked("O",tmpstr2) & " > แสดงในกรมธรรม์</td></tr>" & _
				   "<tr><td></td><td><input type=radio id=taxaddresstype name=taxaddresstype value=O " & getchecked("O",tmpstr4) & " > พิมพ์ใบกำกับ</td></tr>" & _
				   "<tr><td></td><td>เส้น<a href='javascript:getroute(""off"");' ><img src=images/search.gif border=0></a>" & replace(replace(getCombo("offrouteid", "cmbRoute",""),"430px","200px"),"onchange=document.getElementById('myform').submit();","onchange=setrouteidsub('off');") & "</td></tr>" & _
				   "</table>"


	Response.write "</td>"
	Response.write "<td width = 250  bgcolor=#E7E7E7>"

	addtemp = getleaddata("contactaddress")
	if request("cmd") ="" then

		call buildaddress
	end if

	response.write addtemp & "<br>"
	Response.write "<table>" & _
				   "<tr valign=top><td>ชื่อ</td><td colspan=3>" & getComboTXT("contactprefix", "cmbprefix",mapPrefix)    & "<br>" &  inputTagMax("contactname","T",getleaddata("leadname"),10,100) & inputTagMax("contactsurname","T",getleaddata("leadsurname"),12,100)  & "</td></tr>" & _
				   "<tr><td>เลขที่ </td><td>" & inputTagMax("contactno","T",addline1,15,100) & "</td><td align=right>หมู่</td><td> " & inputTagMax("contactgroup","N","",2,50) & "</td></tr>" & _
				   "<tr><td>หมู่บ้าน/ตึก </td><td>" & inputTagMax("contactbuilding","T","",15,100) & "</td><td align=right>ซอย</td><td>" & inputTagMax("contactbystreet","T","",10,100) & "</td></tr>" & _
				   "<tr><td>ตรอก </td><td>" & inputTagMax("contactpath","T","",15,100) & "</td><td align=right>ถนน</td><td>" & inputTagMax("contactstreet","T","",10,255) & "</td></tr>" & _
				   "<tr><td>จังหวัด</td><td>" & getCombo("contactprovince", "cmbProvince",addprovince) & "</td><td>" & getCombo("contactdistricttype", "cmbdistricttype","A") & "</td><td>" & inputTagMax("contactdis","T",addDistrict,10,255) & "</td></tr>" & _
				   "<tr><td>ตำบล/แขวง</td><td colspan=3>" & inputTagMax("contactarea","T",addline2,15,255) &  " ป." & inputTagMax("contactzipcode","T",addzip,5,5) & "&nbsp;<a href='javascript:getpost(""contact"");' ><img src=images/search.gif border=0></a></td></tr>" & _
				   "<tr><td>ติดต่อ</td><td colspan=3>" & inputTagMax("contactcontact","T","",35,100) & "</td></tr>" & _
				   "<tr><td>โทรศัพท์</td><td colspan=3>" & inputTagMax("contactaddphone","T",p1,35,255) & " <input type=hidden id=contactroute name=contactroute value=" & request("contactroute")  & "></td></tr>" & _
				   "</table>" & _

				   "<table>" & _
				   "<tr><td></td><td width=200><input type=radio id=contactaddresstype name=contactaddresstype value=C " & getchecked("C",tmpstr1) & "  onclick=""setrouteid('contact');"" > ติดต่อ </td><td width=100 align=right><input type=radio id=contactaddresstype name=contactaddresstype value='' " & getchecked("",tmpstr1)  & " ><font color=#707070> ไม่กำหนด </td></tr>" & _
				   "<tr><td></td><td width=200><input type=radio id=postaddresstype name=postaddresstype value=C " & getchecked("C",tmpstr5) & " > ไปรษณีย์</td><td width=100 align=right><input type=radio id=postaddresstype name=postaddresstype value='' " & getchecked("",tmpstr5) & " ><font color=#707070> ไม่กำหนด </td></tr>" & _
				   "<tr><td></td><td width=200><input type=radio id=policyaddresstype name=policyaddresstype value=C " & getchecked("C",tmpstr2) & " > แสดงในกรมธรรม์</td><td width=100 align=right><input type=radio id=policyaddresstype name=policyaddresstype value='' " & getchecked("",tmpstr2) & " ><font color=#707070> ไม่กำหนด </td></tr>" & _
				   "<tr><td></td><td width=200><input type=radio id=taxaddresstype name=taxaddresstype value=C " & getchecked("C",tmpstr4) & " > พิมพ์ใบกำกับ</td><td width=100 align=right><input type=radio id=taxaddresstype name=taxaddresstype value='' " & getchecked("",tmpstr4) & " ><font color=#707070> ไม่กำหนด</td></tr>" & _
				   "<tr><td></td><td colspan=2>เส้น<a href='javascript:getroute(""C"");' ><img src=images/search.gif border=0></a>" & replace(replace(getCombo("contactrouteid", "cmbRoute",""),"430px","200px"),"onchange=document.getElementById('myform').submit();","onchange=setrouteidsub('contact');") & "</td></tr>" & _
				   "</table>"

	Response.write "</td>"
	Response.write "</tr>"
	response.write "<tr><td colspan='4'><div class=HMed>&nbsp;*** ระบุเส้นทางจัดส่งตามที่อยู่นี้ <a href='javascript:getroute1();' ><img src=images/search.gif border=0></a><br> &nbsp;&nbsp;&nbsp;" & getCombo("routeid", "cmbRoute","") & "</div></td></tr>"	
	Response.write "</table>"
	
	dim strcurrentdate
	strcurrentdate = deliverydate
	if isnull(strcurrentdate) or strcurrentdate ="" then
		strcurrentdate = datefix(date())
	else
		strcurrentdate = datefix(strcurrentdate)
	end if
	
	Response.write "<table>"
	Response.write  "<tr valign=top><td width=10></td>"
	Response.write	"<td class=HMed><br> หมายเหตุฝ่ายขาย: <br>" & inputTagmax("remarkadmin","T","",70,255) & "<br>" & _
					"วันที่นัดจัดส่งวางบิล <br><script>DateInput('deliverydate', true, 'DD/MM/YYYY', '" &  strcurrentdate & "')</script>" & _	
					"</td>"
	Response.write	"<td class=HMed colspan=2><br><font color=#E01010>&nbsp;หมายเหตุการจัดส่ง <span style='margin-left:110px;'><a href='javascript:gethashtag("""");' ><b>เหตุผล</b></a></span><br>" & _
					"<textarea name=contactperson rows=3 cols=40 style='bgcolor=#A0A000;' onkeypress=""return imposeMaxLength(this, 255);"" onblur='check_ContactPerson(this)'>" & _
					request("contactperson") & _
					"</textarea></td>"
	Response.write "</table>"

	
	Response.write "<br>"
	Response.write "<table><tr><td><div class=HAlert>&nbsp;<img src=images/bullet2.gif> ข้อมูลผู้เอาประกัน </div></td>" & _
                    "<td>&nbsp;<input type=checkbox id=chkSameCus name=chkSameCus class=cus>&nbsp;ลูกค้าและผู้เอาประกันคนเดียวกัน</td></tr></table>"

	Response.write "<table><tr><td width = 800 height=2 bgcolor =#E0E0E0></td></tr></table>"
	
	Response.write "<table>" & _
				   "<tr valign=top>" & _
				   "<td width=10></td>" & _
				   "<td width=50> <b><font color=red>ผู้เอาประกัน</b></td>" & _
				   "<td width=900 colspan=2 >" & getComboTXT("insureprefix", "cmbprefix",getleaddata("leadprefix"))  & inputTag("insurename","T",getleaddata("leadname"),10) & inputTag("insuresurname","T",getleaddata("leadsurname"),10) & _
				   "</td></tr>" & _
                   "<tr><td width=10></td><td width=70>บัตรปชช</td><td colapan=2>" & inputTag("citizenid","T",rslead("citizenid"),15)& ""
					response.write " <img src='images/noidcard.png' width='15px' height='15px' onclick=""getCitizenDummy('citizenid')"" style='cursor:pointer;' title='Auto เลขบัตรประชาชน Dummy' />" & _
                   "</tr>" &  _
				   "<tr><td width=10></td><td width=70>วันเกิด</td>" & _ 
						"<td colapan=2><script>DateInput('insurebirthdate', true, 'DD/MM/YYYY','"& insuredate &"')</script></td>" & _
				   "</tr>" & _ 
				   "</table>"
				   
	
	Response.write "<br>"
	Response.write "<div class=HAlert>&nbsp;<img src=images/bullet2.gif> ข้อมูลกรมธรรม์</div>"
	Response.write "<table width='100%'><tr>" & _
				   "<td align=right>" & _
				   "</tr></table>"
				   
	dim sql_sup
	sql_sup =  " select s.suppliercode , s.suppliername FROM(xininsure.product@backoffice p INNER JOIN xininsure.producttype@backoffice t ON p.producttype = t.producttype) INNER JOIN xininsure.supplier@backoffice s ON p.supplierid = s.supplierid WHERE ( ( ( t.productgroup ) = 'DIRECT' ) AND ( ( p.productstatus ) = 'A' ) ) OR ( ( ( p.directmode ) = 'Y' ) ) GROUP BY s.suppliercode , s.suppliername ORDER BY s.suppliercode "
	Set objrst = objConn.Execute(sql_sup)

	Response.write "<table><tr><td width=10></td>" 
	response.write "<td class=HMed2>บริษัทประกัน</td>"
	response.write "<td><input type='hidden' name='suppliercode' id='suppliercode' value='"&request("suppliercode")&"' /><select id=suppliercode_sel name=suppliercode_sel  style='width:300;color:#2233AA;font-weight:bold;' onChange='selectsupplier()' disabled > " & _
						"<option>เลือกบริษัทประกัน</option>" 
						do until objrst.eof  %> 
							<option value='<% Response.write objrst("suppliercode")%>' <% if(request("suppliercode")=objrst("suppliercode"))then response.write "selected /"%>><% Response.write objrst("suppliercode") & ":" & objrst("suppliername") %></option>
						<% objrst.movenext
						loop
	Response.write "</select></td>" 

	response.write "<td class=HMed2>MODE สินค้า</td>"				
	response.write "<td><input type='hidden' name='productid' id='productid' value='' /><select id=productid_sel name=productid_sel  style='width:300;color:#2233AA;font-weight:bold;' disabled>" & _
						"<option value>เลือกMODE สินค้า</option>" 
	Response.write 	"</select></td>"

	Response.write 	"</tr></table>"
					
	dim supplierdate , expirydate , tmpdate , setExpirydate
	If request("policydate") <> "" Then
		tmpdate = dateadd("d",1, request("policydate"))
	Else
		tmpdate = dateadd("d",1, Date())
	End If
	If request("expirydate") <> "" Then
		'setExpirydate = formatDateEdit(dateadd("yyyy",1,request("expirydate") ))
		setExpirydate = request("expirydate")
	Else
		setExpirydate = formatDateEdit(dateadd("yyyy",1,tmpdate ))
	End If

	expirydate = setExpirydate
	supplierdate = formatDateEdit(Date())	
	
	Response.write "<input type=hidden id=supplierdate name=supplierdate value='"& supplierdate &"'>"			   
	Response.write "<table><tr valign=top><td width=10></td>" & _			   
				   "<td align=right class=HMed2>คุ้มครอง </td><td><b><script>DateInput('policydate', true, 'DD/MM/YYYY', '" &  policydate & "')</script></td>" & _
				   "<td align=right class=HMed2>สิ้นสุด </td><td><b><script>DateInput('expirydate', true, 'DD/MM/YYYY', '" &  expirydate & "')</script></td>" 
				   
	Response.write "<td class=HMed2>ทุน </td><td class=HMed width=80>" & inputTagNoCal("cover","N",0,20) & "</td>"
	Response.write "</tr>"  & _
				    "</table>"
	Response.write "<br>"
	
	Response.write "<table>" & _ 
            "<tr height=27>" & _ 
                "<td colspan=15><div class=HAlert>&nbsp;<img src=images/bullet2.gif> มูลค่ากรมธรรม์</div></td>" & _ 
            "</tr>" & _
			
            " <tr style='color:#545454;background-color:#DFDFDF;'>" & _
				"<td>" & _ 
				"<td align='center'>เบี้ยสุทธิ</td>" & _ 
				"<td align='center'>อากร</td>" & _ 
                "<td align='center'>ภาษี</td>" & _ 
                "<td align='center'>เบี้ยประกันรวม</td>" & _ 
                "<td align='center'>ส่วนลด นตร</td>" & _ 
				"<td align='center'>ส่วนลดคอม</td>" & _ 
				"<td align='center'>%</td>" & _ 
				"<td align='center'></td>" & _ 
				"<td align='center'>ส่วนลดอื่น %</td>" & _ 
                "<td align='center'></td>" & _ 
                "<td align='center'>คิดเป็นเงิน</td>" & _ 
                "<td align='center'>ค้างชำระ</td>" & _ 
                "<td align='center'>TAX %</td>" & _ 
                "<td align='center'>บาท</td>" & _ 
            "</tr>" & _  
            "<tr height=27><td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("amount","N",0,9) & "</td>" & _ 
 			  "<td style=vertical-align:top;>" & inputTagNoCal("duty","N",0,5) & "</td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("vat","N",0,5) & "</td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("netvalue","R",0,9) & "</td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("extdissale","N",0,9) & "</td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("extdiscom","N",0,9) & "</td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("extdiscom_percent","N",0,3) & "</td>" & _ 
              "<td></td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("discountother_percent","N",0,9) & "<input type=hidden name=discountother id=discountother ></td>" & _ 
              "<td></td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("netamount","R",0,9) & "</td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("balance","N",0,9) & "</td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("txttaxrate","R",0,3) & "</td>" & _ 
              "<td style=vertical-align:top;>" & inputTagNoCal("taxvalue","R",0,9) & "</td>" & _ 
       "</table><br>"
	   
	Response.write "<input type=hidden id=chk_oDetail name=chk_oDetail value="& request("chk_oDetail") &">"
	Response.write "<div style='overflow:hidden;'>&nbsp;"  				
	Response.write "<a class=tablinks href='javascript:openDetail(""event"",""detail_other"");'><b> :ข้อมูลเพิ่มเติม</a>&nbsp;"
	Response.write "<a class=tablinks href='javascript:openDetail(""event"",""detail_home"");'><b> :ข้อมูลบ้าน</a>&nbsp;"
	Response.write "<a class=tablinks href='javascript:openDetail(""event"",""detail_car"");'><b> :ข้อมูลรถ</a>&nbsp;"
	Response.write "</div><br>"
	
	response.write "<div id=detail_other class=tabcontent>"   '------- ข้อมูลอื่นๆ
	Response.write "<table><tr><td><div class=HAlert>&nbsp;<img src=images/bullet2.gif> ข้อมูลเพิ่มเติม</div></td></tr></table>"
	Response.write "<table width='100%'><tr>" & _
				   "<td align=right>" & _
				   "</tr></table>"
	Response.write "<table><tr><td width=10></td> " & _
				   "<td style=width:40px;>Extra</td> " & _
				   "<td style=width:30px;>เฟอร์นิเจอร์</td> " & _
				   "<td style=width:130px;>" & inputTagNoCal("coverv3rd","N",0,20) & "</td> " & _
				   "<td style=width:50px;>สิ่งปลูกสร้าง</td> " & _
				   "<td style=width:130px;>" & inputTagNoCal("coverv3rdasset","N",0,20) & "</td> " & _
				   "<td style=width:alto;>stock สินค้า</td> " & _ 
				   "<td style=width:150px;>" & inputTagNoCal("coverv3rdtime","N",0,20) & "</td> " & _
				   "<td style=width:50px;></td></tr> " & _ 
				   "</tr></table>"
	Response.write "<table><tr><td width=10></td> " & _			   
				   "<td><b>MARINE</b></td> " & _
				   "<td>ชื่อลูกค้า</td> " & _
				   "<td>" & inputTag("driver1name","T","",20) & "</td> " & _
				   "<td>invoice &nbsp;&nbsp;</td> " & _
				   "<td>" & inputTag("driver1licenseid","T","",20) & "</td> " & _
				   "<td>พนักงานขาย&nbsp;</td> " & _
				   "<td><input type=text value="& session("staffcode")&":"& session("staffname") &" readonly/></td>" & _
				   "<td>ทีมขาย</td> " & _
				   "<td><input type=text value="& session("departmentcode")& " readonly/></td>" & _
				   "<td>&nbsp;</td></tr>" & _
				   "<tr><td width=10></td></tr>" & _
				   "</tr></table>"
	
	'--- อาชีพ ---
	Response.write "<table width=800><tr><td class=HAlert>&nbsp;<img src=images/bullet2.gif> ข้อมูลอาชีพ </td></tr></table>"
	Response.write "<table><tr><td width = 800 height=2 bgcolor =#E0E0E0></td></tr></table>"

	Response.write "<table><tr><td width=10></td>" & _
					   "<td width=120 class=HSmall2></td>"  & _
					   "<td width=130>การศึกษาสูงสุด</td><td>" &  inputTag("graduation","T","",18) & "</td>" & _ 
 			       "</tr></table>"
				   
	Response.write "<table><tr><td width=10></td>" & _
				   "<td width=120 class=HSmall2></td>"  & _
				   "<td width=130>ลักษณะธุรกิจ</td><td width=125>" &  inputTag("businesstype","T",request("businesstype"),18) & "</td>" & _
	 			   "<td width=70>ประเภทธุรกิจ</td><td>" & inputTag("businesscategory","T",request("businesscategory"),20) & "</td> "  & _
 			       "</tr></table>"

	Response.write "<table><tr><td width=10></td>" & _
				   "<td width=120 class=HSmall2></td>"  & _
				   "<td width=130>ตำแหน่ง</td><td>" &  inputTag("position","T","",18) & "</td>" & _
	 			   "<td width=70>อายุงาน</td><td width=100 >" & inputTag("experience","N","",10) & " ปี</td> "  & _
 			       "</tr></table>"

	Response.write "<table><tr><td width=10></td>" & _
				   "<td width=120 class=HSmall2></td>"  & _
				   "<td width=130>รายได้เจ้าของกิจการ</td><td>" & replace(inputTag("ownerincome","F","0",15),"onchange='check_number(this)'","onchange=check_number(this);CalCheck();") & " บาท </td> "  & _
 			       "</tr></table>"

	Response.write "<table><tr><td width=10></td>" & _
				   "<td width=120 class=HSmall2></td>"  & _
				   "<td width=130>รายได้ผู้ประกอบอาชีพอิสระ</td><td>" & replace(inputTag("freelanceincome","F","0",15),"onchange='check_number(this)'","onchange=check_number(this);CalCheck();") & " บาท </td> "  & _
 			       "</tr></table>"

	Response.write "<table><tr><td width=10></td>" & _
				   "<td width=120 class=HSmall2></td>"  & _
				   "<td width=130>รายได้อื่น(ระบุ)</td><td>" & replace(inputTag("otherincome","F","0",15),"onchange='check_number(this)'","onchange=check_number(this);CalCheck();") & " บาท </td> "  & _
 			       "</tr></table>"			
	
	Response.write "<table><tr><td width=10></td>" & _
				   "<td width=120 class=HSmall2></td>"  & _
				   "<td width=130>ช่วงเงินเดือน</td><td>" & replace(inputTag("salaryrange","F","0",15),"onchange='check_number(this)'","onchange=check_number(this);CalCheck();") & " บาท </td> "  & _
 			       "</tr></table>"	
				   
	response.write	"</div>" 
					
	response.write "<div id=detail_home class=tabcontent>" '------- ข้อมูลบ้าน
	
	Response.write "<table><tr><td><div class=HAlert>&nbsp;<img src=images/bullet2.gif> รายละเอียดลักษณะอาคาร </div></td>" & _
                   "</tr></table>"
	Response.write "<table><tr><td width = 800 height=2 bgcolor =#E0E0E0></td></tr></table>"
	Response.write "<table><tr><td width=10></td>" & _
	 			   "<td width=160>เลขรหัสประจำบ้านในทะเบียนบ้าน</td>"  & _
				   "<td> " & inputTag("housecodeno","T","",20) & "</td>" &  _
 			       "</tr></table>"

	tmpstr5 = request("ownerstatus")
	Response.write "<table>" & _ 
						"<tr>" & _ 
							"<td width=10></td>" & _ 
							"<td width=120> ผู้เอาประกันเป็น </td>" & _ 
							"<td width=100><input type=radio name=ownerstatus value=OWNER " & getchecked("OWNER",tmpstr5) & " > เจ้าของ </td>" & _ 
							"<td><input type=radio name=ownerstatus value=RENTER " & getchecked("RENTER",tmpstr5) & "  >ผู้เช่า </td>" & _ 
						"</tr>" & _ 
					"</table>"

	dim tmpstr6 , tmpstr7
	tmpstr6 = request("riskcode")
	Response.write "<table><tr><td width=10></td> " & _
					"<td width=120>บ้านอยู่อาศัย </td> " 

	response.write "<td width=100><input type=radio name=riskcode value=HOUSE " & getchecked("HOUSE",tmpstr6) & " > บ้านเดี่ยว </td>" & _
					   "<td width=100><input type=radio name=riskcode value=SEMIHOUSE " & getchecked("SEMIHOUSE",tmpstr6) & " > บ้านแฝด </td>" & _
					   "<td width=120><input type=radio name=riskcode value=TOWNHOUSE " & getchecked("TOWNHOUSE",tmpstr6) & " > ทาวน์เฮ้าส์ </td>" & _
					   "<td width=98><input type=radio name=riskcode value=BUILDING " & getchecked("BUILDING",tmpstr6) & " > ตึกแถว </td>" & _
					   "<td width=98><input type=radio name=riskcode value=CONDO " & getchecked("CONDO",tmpstr6) & " > คอนโด </td>"

	response.write "</tr></table>"
	
	tmpstr7 = request("constcode")
	Response.write "<table><tr><td width=10></td> <td width=120>โครงสร้างอาคาร </td> "

	 Response.write "<td width=100><input type=radio name=constcode value=CONCRETE " & getchecked("CONCRETE",tmpstr7) & " > คอนกรีต </td>" & _ 
					"<td width=120><input type=radio name=constcode value=CW " & getchecked("CW",tmpstr7) & " > คอนกรีตครึ่งไม้ </td>" & _ 
					"<td width=98><input type=radio name=constcode value=WOOD " & getchecked("WOOD",tmpstr7) & " > ไม้ </td>"

    Response.write "</tr></table>"
	Response.write "<table><tr><td width=10></td> <td width=120>จำนวนชั้น </td> "
    Response.write			"<td width=100>" &  getCombo("floorcount", "cmbFloor", request("floorcount")) & "</td>"
	Response.write "</tr></table>"
	
	Response.write "<table><tr><td width=10></td> <td width=120>จำนวนคูหา / หลัง </td> "
    Response.write			"<td width=100>" & inputTag("unitcount","UC", request("unitcount"),10) & "</td>"
	Response.write "</tr></table>"
	
	Response.write "<table><tr><td width=10></td> <td width=120> พื้นที่ใช้สอย </td>" 
	Response.write "<td width=140> กว้าง  " & inputTag("assetwidth","F",request("assetwidth"),10) & "  เมตร </td> "
	Response.write "<td width=140> ยาว  " & inputTag("assetlength","F",request("assetlength"),10) & "  เมตร </td> " 
	Response.write "<td width=210> รวมพื้นที่ใช้สอย  " & inputTag("assetarea","F",request("assetarea"),10) & "  ตารางเมตร </td> " 
	Response.write "</tr></table>"
	
	Response.write "<table><tr><td width=10></td> <td width=120> </td>"
    Response.write "</tr></table>"

	Response.write "<table><tr><td width=10></td> <td width=120>ราคาอาคาร</td>" & _
	 			       "<td>" & inputTag("buildingamount","N", request("buildingamount"),15) & " บาท (โดยประมาณ ไม่รวมรากฐานและที่ดิน)</td> "  & _
 			           "</tr></table>"

	Response.write "<table><tr><td width=10></td>" & _
				       "<td width=120>ราคาทรัพย์สินในอาคาร</td>" & _
	 			       "<td>" & inputTag("contentamount","N", request("contentamount"),15) & " บาท กรณีมากกว่า 300000 บาท โปรดระบุ</td> "  & _
 			           "</tr></table>"

	Response.write "<table><tr><td width=10></td>" & _
				       "<td width=120></td>" & _
				       "<td>" & inputTag("contentremark","T", "",107) & "</td>" & _
				       "</tr></table>"
	
	Response.write "<table width=800><tr><td class=HAlert>&nbsp;<img src=images/bullet2.gif> ลักษณะที่อยู่ </td></tr></table>"				
	Response.write "<table><tr><td width = 800 height=2 bgcolor =#E0E0E0></td></tr></table>"
    tmpstr4 = request("homestatus")
	Response.write "<table>" & _
				   "<tr><td width=10></td><td width=120> ผู้สมัครเป็น </td><td width=200><input type=radio name=homestatus value=OWNER " & getchecked("OWNER",tmpstr4) & " > บ้านตนเอง </td><td width=60>ผ่อนเดือนละ</td><td>" &  inputTag("ownerpay","N","",12) & "</td><td> บาท</td></tr>" & _
				   "<tr><td width=10></td><td width=120></td><td width=200><input type=radio name=homestatus value=PARENTHOME " & getchecked("PARENTHOME",tmpstr4) & " > บ้านบิดา/มารดา </td><td width=60></td><td width=100></td></tr>" & _
				   "<tr><td width=10></td><td width=120></td><td width=200><input type=radio name=homestatus value=COUSINHOME " & getchecked("COUSINHOME",tmpstr4) & " > บ้านญาติ/พี่น้อง/บุคคลอื่น </td><td width=60></td><td width=100></td></tr>" & _
				   "<tr><td width=10></td><td width=120></td><td width=200><input type=radio name=homestatus value=WELFAREHOME " & getchecked("WELFAREHOME",tmpstr4) & " > บ้านสวัสดิการ </td><td width=60></td><td width=100></td></tr>" & _
				   "<tr><td width=10></td><td width=120></td><td width=200><input type=radio name=homestatus value=RENTER " & getchecked("RENTER",tmpstr4) & " > บ้านเช่า </td><td width=60>เช่าเดือนละ</td><td>" &  inputTag("RENTERPAY","N","",12) & "</td><td> บาท</td></tr>" & _
				   "</table>"
	Response.write "<table>" & _
				   "<tr><td width=10></td><td width=120> อาศัยมานาน </td><td width=40>" &  inputTag("home_year","N","",5) & "</td><td width=20>ปี</td><td width=40>" &  inputTag("home_month","N","",5) & "</td><td width=20>เดือน</td></tr>" & _
				   "</table>"
	Response.write "</div>"
					
	response.write	"<div id=detail_car class=tabcontent>" 
	
	Response.write "<table width=800><tr><td class=HAlert>&nbsp;<img src=images/bullet2.gif>ข้อมูลป้ายทะเบียน</td></tr></table>"
	Response.write "<table><tr><td width = 800 height=2 bgcolor =#E0E0E0></td></tr></table>"
	Response.write "<table width='100%'>" & _ 
					"<tr><td width=10></td>" & _ 
					"<td><b>จำนวนป้ายทะเบียน </b> &nbsp;<input style=text-align:right;width:50; type=text name=countplateid id=countplateid maxlength=3 onchange=check_number(this); value="& request("countplateid") &" > &nbsp;</td>"& _
					"</tr>" & _ 
					"</table>"
	
	Response.write "<table><tr>" 		
	if countplateid = "" then 
		countplateid = 0
	end if 
			for i = 1 to countplateid 
				if i < 10 and i <> "" then
					response.write "<tr><td width=10></td>"
					response.write "<td>ป้ายทะเบียนรถคันที่ "& i &" </td>" & _ 
									"<td>" & inputTag("PLATEID_00"& i &"","P","",19) & "</td>" & _ 
									"<tr>"
				elseif i >= 10 and i < 100  Then
					response.write "<tr><td width=10></td>"
					response.write "<td>ป้ายทะเบียนรถคันที่ "& i &" </td>" & _ 
									"<td>" & inputTag("PLATEID_0"& i &"","P","",19) & "</td>" & _ 
									"<tr>"
				elseif i = 100 then 					
					response.write "<tr><td width=10></td>"
					response.write "<td>ป้ายทะเบียนรถคันที่ "& i &" </td>" & _ 
									"<td>" & inputTag("PLATEID_"& i &"","P","",19) & "</td>" & _ 
									"<tr>"
				end if
			next 
	Response.write  "</tr></table>"
	Response.write	"</div>"
	
	'----- ผู้รับผลประโยชน์ ----- 
		Response.write "<br>"
		Response.write "<table width=800><tr><td class=HAlert>&nbsp;<img src=images/bullet2.gif>ข้อมูลผู้รับผลประโยชน์</td></tr></table>"
		Response.write "<table><tr><td width = 800 height=2 bgcolor =#E0E0E0></td></tr></table>"
		Response.write "<table><tr><td width=10></td>" & _
						   "<td width=50>1. ชื่อ</td><td> " & inputTag("nominee","T","",20) & "</td>" & _
						   "<td>สัมพันธ์ " & replace(getCombo("relationcode", "cmbrelation",""),"<select ", "<select onchange='relaother()'") & "</td>" & _
						   "<td>อื่น.. " & inputTag("relationtext","T","",16) & "</td>" 		 
		Response.write "</tr><tr></table>"
	    for i = 2 to 2
				Response.write "<table><tr><td width=10></td>" & _
							   "<td width=50>" & i & ". ชื่อ</td><td> " & inputTag("nominee" & i ,"T","",20) & "</td>" & _
							   "<td>สัมพันธ์ " & replace(getCombo("relationcode" & i, "cmbrelation",""),"<select ", "<select onchange='relaotherext("""  & i & """)'") & "</td>" & _
							   "<td>อื่น.. " & inputTag("relationtext" & i,"T","",16) & "</td>" & _
							   "</tr><tr></table>"
	    next
	    if nomineecount > 1 then
			Response.write "<table><tr>" & _
						   "<td align=right width=750>TOTAL</td>"	 & _
						   "<td align=right width=50><div id=xtotal>" & x & "</div></td>"	 & _
						   "</tr><tr></table>"
		end if
	' end nominee
	
	Response.write "<table><tr><td width = 800 height=2 bgcolor =#E0E0E0></td></tr></table>"					
	
	Response.write "<table width=800><tr><td class=HAlert>&nbsp;<img src=images/bullet2.gif>การชำระเงิน</td></tr></table>"
	Response.write "<table><tr valign=top><td bgcolor=white width=10></td>"
	response.write"<td align=center bgcolor=#A0B0C0 width=350><b><font color=white>ข้อมูลบัตรเครดิต "
	response.write"</td>"
	response.write"<td align=center bgcolor=#A0B0C0 width=350><b><font color=white>ข้อมูลการหักบัญชี</td>"
	Response.write "</tr>"

	Response.write "<tr><td bgcolor=white width=10></td><td bgcolor=#F0F0F0>"
	tmpstr1 = request("cardtype")
	Response.write "<table cellpadding=0 cellspacing=0><tr valign=top>" & _
					   "<td  bgcolor=#F0F0F0  valign=top>" & _
					   "<table>" & _ 
					   "<tr><td>ธนาคาร  </td><td> " & replace(getCombo("cardbankid","cmbBank",""),"<select","<select onChange='COST(0,0)' ") &"</td></tr>" & _
					   "<tr><td>ชื่อผู้ถือบัตร  </td><td> " & inputTag("cardholdername","T","",30) & "</td></tr>" & _
					   "<tr><td>ความสัมพันธ์  </td><td> " & replace(getCombo("cardholderrelation", "cmbCardholderRelation",""),"<select","<select onchange='RelationCard()'") & "</td></tr>" & _
					   "<tr><td>  </td><td> " & inputTagMax("cardholderrelation_txt","T","",30,55) & "</td></tr>" & _			   
					   "<tr valign=top><td>บัตรเครดิต</td><td> "  & _
					   "<table>" & _ 
					   "<tr>" & _
							"<td><input type=radio id=cardtype name=cardtype value=visa " & getchecked("visa",tmpstr1) & " >VISA  </td>" & _
						   "<td><input type=radio id=cardtype name=cardtype value=master " & getchecked("master",tmpstr1) & " >MASTER  </td>" & _
						   "<td><input type=radio id=cardtype name=cardtype value=american " & getchecked("american",tmpstr1) & " >AMERICAN </td>" & _
						   "</tr><tr>" & _
						   "<td><input type=radio id=cardtype name=cardtype value=dinner " & getchecked("dinner",tmpstr1) & ">DINNER  </td>" & _
						   "<td><input type=radio id=cardtype name=cardtype value=jcb " & getchecked("jcb",tmpstr1) & ">JCB  </td>" & _
						   "<td><input type=radio id=cardtype name=cardtype value=bank " & getchecked("bank",tmpstr1) & " >-<i>NA-BANK- </td>" & _
					   "</tr><tr>" & _
					   "<td></td>" & _
					   "<td></td>" & _
					   "<td></td>" & _
					   "</tr></table>" & _
					   "</td></tr>" & _
					   "<tr><td>เลขที่บัตรเครดิต  </td><td> " & inputTagMax("cardnumber","N","",18,16) & "</td></tr>" & _
					   "<tr><td>วันหมดอายุ(เดือน/ปี)</td><td> " & inputTagmax("cardexpiry1","N","",3,2) & "/" & inputTagmax("cardexpiry2","N","",3,2) & "</td></tr>" & _
					   "</table>" & _
					   "</td>" & _
					   "</tr></table>"
		Response.write "</td><td bgcolor=#F0F0FF valign=top >"

		Response.write "<table>" & _
							   "<tr><td>ธนาคาร  </td><td> " & replace(getCombo("bankid", "cmbBank",""),"<select","<select onChange='COST(0,0);'") & "</td></tr>" & _
							   "<tr><td>สาขา  </td><td> " & inputTag("bankbranch","T","",30) & "</td></tr>" & _
							   "<tr><td>ชื่อบัญชี  </td><td> " & inputTag("bankaccountname","T","",30) & "</td></tr>" & _
							   "<tr><td>ธนาคาร  </td><td> " & getCombo("bankaccounttype", "cmbBankaccounttype","S") & "</td></tr>" & _
							   "<tr><td>เลขที่บัญชี  </td><td> " & inputTagMax("bankaccount","N","",12,10) & "</td></tr>" & _
							   "</table>"
		Response.write "</td></tr></table>"
	
	Response.write "<table width=800>"
	
	Response.write "<input type = hidden id= installmentold name= installmentold value=" & installment & ">"
	totalAmount = cnum(request("netamount"))
	Response.write "<tr><td width=400></td>"
	Response.write "<td colspan=6 align=right width=150 class=halert><b>ยอดรวมสุทธิ " & showlockNo1("totalamount",TotalAmount,10) & "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>"
	response.write "</tr>"	
	runamount=0
			for i = 1 to installment 
				if i = 1 then
					Response.write "<tr><td width=400></td>"
					Response.write "<td colspan=6 class=halert align=right>เลือกชำระ"
					Response.write " " & inputTag("installment","N",installment, 5)   & " งวด "
					%>
					<span id="sri"></span><!--แสดงไอคอน ศรีสวัสดิ์-->
					<%
					response.write "</td><td width=70></td></tr>"
				end if
				Response.write "<tr><td width=500></td><td>" & i & "</td>"
				Response.write "<td>" & replace(getCombo("INSTALLTYPE" & i, "cmbPaymentType","C"),"<select","<select onChange='COST(" & i & ",this.value);'") & "</td>"

			if ( cdbl(nvl(Request("installmentold"),0)) <> cdbl(nvl(request("installment"),0))  )   then   'or cdbl(Request("finalamount")) <> cdbl(totalAmount)
				if i = installment then
					setamount = request("netamount") - runamount
				else
					setamount = cdbl(request("netamount") / installment)
				end if

				Response.write "<td>" & inputTag("INSTALLDATE" & i ,"D", formatDateEdit( dateadd("M",i-1,todate(deliverydate))),10)  & "</TD>"

			else
				if i = installment then
					setamount = request("netamount") - runamount
				else
				    setamount = request("INSTALLAMOUNT" & i )
				end if
			    Response.write "<td>" & inputTag("INSTALLDATE" & i ,"D",formatDateEdit( dateadd("M",i-1,todate(deliverydate))),10)  & "</TD>"
			end if

			runamount =(runamount)+ (setamount)

			if i = installment then
		       Response.write "<td class=hmed align=right>" & formatnumber(cNum(setamount),2) & "</td>"
			   Response.write "<input type = hidden id=INSTALLAMOUNT" & i & " name=INSTALLAMOUNT" & i & "  size=10 value='" & formatnumber(cNum(setamount),2) & "'  onblur='check_number(this)'  style='text-align:right;'>"
		    else
		       Response.write "<td><input type = text id=INSTALLAMOUNT" & i & " name=INSTALLAMOUNT" & i & "  size=10 value='" & formatnumber(cNum(setamount),2) & "'  onblur='check_number(this)'  style='text-align:right;'></td>"
		    end if

			Response.write "<td><select name=RECEIVECOST" & i & " id=RECEIVECOST" & i & " width: 200px><option value=>** กรุณาเลือก cost code **</option></select></td><td rowspan='2' width='27px'><span id='TOTALLOAN' style='display:none;'></span></td>"

			Response.write "</TR>"

			dim vtype, vcost
			vtype = request("INSTALLTYPE" & i)
			vcost = request("RECEIVECOST" & i)

			if vtype = "K" or vtype = "R" or vtype = "G" then
				response.write "<script>COST('" & i & "','" & vtype & "');</script>"
				response.write "<script>selCOST('" & i & "','" & vcost & "');</script>"
			end if

			response.write "</tr>"		
	next				
	
	Response.write "</table>" 
	Response.write "<table><tr><td width = 800 height=2 bgcolor =#E0E0E0></td></tr></table>"		
	Response.write "<table><tr><td width=760 align=right>"
	
	if not isnull(showQuestion) then
		call DisplayProductQuestion
	end if

	if request("read") <> "1" then

		select case request("cmd")
		case "1"

			if isnull(submiterror) then
			   response.write "<font style='font: 11pt tahoma; color:#CC0000; '><b>*** ก่อนยืนยัน กรุณาทวนรายการกับลูกค้าให้ถูกต้องก่อน </b></font> <a href=#><img src=images/btnconfirm.jpg onclick=submit() border=0></a>"
			   Response.write "<input type = hidden name=cmd value=2>"
			else
			   response.write "<a href=#><img src=images/btncheck.jpg onclick=submit() border=0></a>"
			   Response.write "<input type = hidden name=cmd value=1>"
			end if
		case "2"
			if isnull(submiterror) then
			   response.write "<a href=#><img src=images/btnconfirm.jpg onclick=submit() border=0></a>"
			   Response.write "<input type = hidden name=cmd value=2>"
			else
			   response.write "<a href=#><img src=images/btncheck.jpg onclick=submit() border=0></a>"
			   Response.write "<input type = hidden name=cmd value=1>"
			end if

		case else

		   response.write "<a href=#><img src=images/btncheck.jpg onclick=submit() border=0></a>"
		   Response.write "<input type = hidden name=cmd value=1>"

		end select

	end if

	Response.write "</td></tr></table>"
	Response.write "</form >"

sub getProductInfo
	dim rs
	dim sql
	dim idx, runsum, runamount, setamount

	paymentDr=0
	paymentCR=0

	sql= " SELECT producttype, productname, questioncode , supplierid, netamount+  " & _
		 "        ( select nvl(sum(x.netamount),0)  " & _
         "                     from   TQMSALE.productchild c, TQMSALE.product x " & _
         "                     where  c.productidchild =x.productid and c.productid = p.productid ) netamount, " & _
		 "        (select max(confignumber) from  TQMSALE.productconfig where productid = p.productid and configcode = 'NOMINEECOUNT') NOMINEECOUNT, " & _
		 "        (select max(configtext) from  TQMSALE.productconfig where productid = p.productid and configcode = 'GETCUSTOMER') GETCUSTOMER, " & _
		 "        (select max(configtext) from  TQMSALE.productconfig where productid = p.productid and configcode = 'GETCUSTOMERNOMINEE') GETCUSTOMERNOMINEE, " & _
		 "        (select max(configtext) from  TQMSALE.productconfig where productid = p.productid and configcode = 'GETNOMINEE') GETNOMINEE, " & _
		 "        (select max(configtext) from  TQMSALE.productconfig where productid = p.productid and configcode = 'GETADDRESSSPLIT') ADDRESSSPLIT, " & _
		 "		  TQMSALE.getwebschedule(productid, '"  & request("pmode") &  "') strx " & _
		 " FROM TQMSALE.PRODUCT p where productid =" & request("pid")


	'sql= "SELECT productname, netamount FROM TQMSALE.PRODUCT where productid =" & request("pid")
	debug(sql)

	set rs = objConn.Execute(sql)
	if  not rs.eof then
	    nomineecount = rs("nomineecount")
	    if isnull(nomineecount) then
	       nomineecount = 1
	    else
	       nomineecount = cint( rs("nomineecount") )
	    end if

		getcustomer = rs("getcustomer")
		getcustomernominee = rs("getcustomernominee")
		getnominee = rs("getnominee")

		producttype = rs("producttype")
	    showquestion = rs("questioncode")
		productname = rs("productname")
		netamount = rs("netamount")
		payschedule = rs("strx")
		supplierid = rs("supplierid")
        addresssplit = rs("addresssplit")

		'--- If you change the tag, please make sure your detect properly
		'
		'
	    if instr(1,payschedule,"<u>D") > 0 then paymentDr =1
	    if instr(1,payschedule,"<u>R") > 0 then paymentCr =1
	    if instr(1,payschedule,"<u>E") > 0 then paymentDC =1

	end if

	rs.close

	'---count installment
	sql = " SELECT nvl(max(installment),0) " & _
		  " FROM   TQMSALE.PRODUCTPAYMENT P "  & _
		  " WHERE  P.PRODUCTID = " & request("pid") &  " AND " & _
		  "        P.PAYMENTMODE = '" & request("pmode") & "' AND nvl(p.installment,0) <> 99 " & _
		  " order by INSTALLMENT "
	set rs = objConn.Execute(sql)
	cnt = cint(rs(0))

	rs.close

	sql = " SELECT P.INSTALLMENT,  " & _
          "        TQMSALE.CHECKHOLIDAY(ADD_MONTHS(to_date('" &  policydate &  "','DD/MM/YYYY') ,NVL(P.PERIODMONTH ,0)) + nvl(P.PERIODDAY,0), P.holidayskip)  dateX,  "  & _
          "        P.NETAMOUNT + "  & _
          "  (   select nvl(sum(y.netamount),0)  " & _
          "       from   TQMSALE.productchild c, TQMSALE.product X , TQMSALE.productpayment y " & _
          "       where  c.productidchild = x.productid and " & _
          "              y.productid =c.productidchild and " & _
          "              c.productid = p.productid and " & _
          "              y.paymentmode = p.paymentmode and " & _
          "              y.installment = p.installment ) netamount " & _
          " FROM   TQMSALE.PRODUCTPAYMENT P "  & _
          " WHERE  P.PRODUCTID = " &   request("pid") &  " AND " & _
          "        P.PAYMENTMODE = '" & request("pmode") & "' AND nvl(p.installment,0) <> 99 " & _
          " order by INSTALLMENT "
    payschedule =""
    debug(sql)
    set rs = objConn.Execute(sql)
    paySchedule = "<table><tr bgcolor=#F0F0F0><td width=30 >งวดที่</td><td width=50>วันที่</td><td width=50 align=right>ยอดเงิน</td></tr>"

    runsum = cdbl(request("newamount"))
	idx = 0
    do until rs.eof
    	if request("newamount") <> "" then

    	   runamount =Round(rs("netamount") * request("newamount") /netamount,2 )
    	   if cint(rs("installment")) = cnt then
    	   	  setamount = runsum
    	   else
    	      setamount = runamount
    	   end if
    	   runsum = runsum - setamount

    	   payschedule = payschedule & "<tr bgcolor=#F7F7F7><td align=right>" & rs("installment") & "</td><td width=70>" & formatDateEdit(rs("datex")) & "</td><td align=right id=install_" & rs("installment")  & ">" & formatnumber(setamount,2) &  "</td></tr>"
    	else
    	   payschedule = payschedule & "<tr bgcolor=#F7F7F7><td align=right>" & rs("installment") & "</td><td width=70>" & formatDateEdit(rs("datex")) & "</td><td align=right id=install_" & rs("installment")  & ">" & formatnumber(rs("netamount"),2) &  "</td></tr>"
    	end if
		idx=idx+1
        rs.movenext
    loop
    paySchedule = paySchedule & "</table><input type=hidden id=install_total value=" & idx &  "> "

	rs.close

	sql = " SELECT P.INSTALLMENT,  " & _
		  "        A.ACTIONCODE || ':' || A.ACTIONNAME actname, " & _
          "        TQMSALE.CHECKHOLIDAY(ADD_MONTHS(to_date('" &  deliverydate &  "','DD/MM/YYYY') ,NVL(P.PERIODMONTH ,0)) + nvl(P.PERIODDAY,0), P.holidayskip)  dateX  "  & _
          " FROM   TQMSALE.PRODUCTACTION P, TQMSALE.ACTION A "  & _
          " WHERE  P.ACTIONID = A.ACTIONID and " & _
          "	       P.PRODUCTID = " &   request("pid") &  " AND " & _
          "        P.PAYMENTMODE = '" & request("pmode") & "' AND nvl(p.installment,0) <> 99 " & _
          " order by nvl(p.periodmonth,0) * 40 + nvl(p.periodday,0) "

    saleaction =""
    idx=1
    debug(sql)
    set rs = objConn.Execute(sql)
    saleaction = "<table><tr bgcolor=#F0F0F0><td width=10>ที่</td><td width=70>วันที่</td><td width=200 >การติดต่อลูกค้า</td></tr>"
    do until rs.eof
       saleaction = saleaction & _
       				 "<tr bgcolor=#F7F7F7><td align=right>" & idx & "</td><td>" & formatDateEdit(rs("datex")) & "</td><td >" & rs("actname") &  "</td></tr>"
       rs.movenext
       idx= idx+1
    loop
    saleaction = saleaction & "</table>"

end Sub

sub getLeadrs

	if session("leadid") <> "" then

		dim sql
		sql= "SELECT l.*,s.nominee , s.relationcode,s.relationtext,s.cardbankid, s.cardholdername, s.cardnumber, s.bankid, s.bankbranch, s.bankaccountname, s.bankaccounttype,bankaccount, " & _
			 " s.child, decode(l.saleid,null,'N','R') saletype ,  l.SALEID extsaleid,  TO_CHAR( decode(s.policydate, null, TQMSALE.CHECKHOLIDAY(trunc(sysdate) +1,'Y' ), add_months(S.POLICYDATE,12) ),'DD/MM/YYYY') POLICYDATE," & _
			 " s.SOULMATEPREFIX, s.SOULMATENAME, s.SOULMATESURNAME, s.SOULMATEID, s.SOULMATEBIRTHDATE, s.SOULMATEgender, s.SOULMATEoccupationid, s.SOULMATEJOBNAME, a.premiumamount, a.assignremark, a.saleid,tqmsale.CHECKITEMLIST((select configvalue from tqmsale.SYSCONFIG where CONFIG = 'REFER_DEPARTMENT'),'"& session("departmentcode") &"') refstaff " & _
			 " from TQMSALE.viewleadpa l, TQMSALE.sale s, TQMSALE.Leadassign a where s.saleid (+) = l.saleid and a.leadid(+) = l.LEADID and  l.leadid=" & session("leadid") & " and  a.leadassignid=" & session("leadassignid")
			 'response.write sql
		set rslead = objConn.Execute(sql)
		'--- newamount ---patt
		' premiumamount = rslead("premiumamount")
		' if newamount = "" then	newamount = premiumamount
	end if
		
end sub

function getValue(FieldName )
	if request(FieldName) = "" then
	   if request("cmd") ="" then
	      getvalue = getleaddata(fieldname)
	   end if
	else
	   getvalue = request(FieldName)
	end if

end function

Function getleaddata(fieldname)
	getleaddata = ""
	if not isempty(rslead) then
		if fieldname ="phonemobile"  or fieldname ="phonecontact" or fieldname ="phonehome" or fieldname ="phoneoffice" then
		   getleaddata =""
		else
			if not rslead.eof then
			   dim i
			   for i =0 to rslead.fields.count -1
				   if ucase(rslead.fields(i).name) = ucase(fieldname) then
					  getleaddata = rslead(i)
					  exit function
				   end if
			   next
			end if
		end if

	end if

end function

Function inputTag(fname,ftype, fvalue, fsize)
	dim strx
	dim valx

	valx = getvalue(fname)

	if len(valx)=0  then
	   valx = fvalue
	end if

	if not isnull(valx) then
	   valx = replace(valx,"'","&#39;")
	   valx = replace(valx,"""", "&#34;")
	end if

	select case ftype
	case "T"
	    strx ="<input type = text id='" & fname & "'  name='" & fname & "'  size=" &  fsize  & "  value='" & valx & "'>"
    case "D"
		'if isdate(valx) then
			'   valx = formatDateEdit(valx)
		'	end if
        strx ="<input type = text id='" & fname & "' name='" & fname & "'  size=" &  fsize  & " maxlength=10  value='" & formatDateEdit3((valx)) & "' title='ใส่เป็น DD/MM/YYYY ปีคศ " & valx & "' onblur='check_date(this)' >"
	case "N"
	    strx ="<input type = text id='" & fname & "' name='" & fname & "'  size=" &  fsize  & "   value='" & formatnumber(cNum(valx),0) & "'  onblur='check_number(this)'  style='text-align:right;' >"
	case "H"
	    strx ="<input type = hidden id='" & fname & "' name='" & fname & "'  size=" &  fsize  & "   value='" & formatnumber(cNum(valx),0) & "'  onblur='check_number(this)'  style='text-align:right;' >"
	case "R"	
        strx ="<input type = text id='" & fname & "' name='" & fname & "'  size=" &  fsize  & " maxlength=10  value='" & formatDateEdit3((valx)) & "' title='ใส่เป็น DD/MM/YYYY ปีคศ " & valx & "' onblur='check_date(this)' style ='background-color:#E2E0E0;' readonly>"
	case "CT"
	    strx ="<input type = text id='" & fname & "'  name='" & fname & "'  size=" &  fsize  & " value='" & valx & "' onblur='check_ContactPerson(this)'>"
	case "UC"
	    strx ="<input type = text id='" & fname & "' name='" & fname & "'  size=" &  fsize  & " maxlength=2  value='" & valx & "' onblur='check_number(this)' style=text-align:right;>"
	case "F"
	    strx ="<input type = text id='" & fname & "' name='" & fname & "'  size=" &  fsize  & " value='" & valx & "' onblur='check_number(this)' style=text-align:right;>"
	case "P"
	    strx ="<input type = text id='" & fname & "'  name='" & fname & "'  size=" &  fsize  & "  value='" & valx & "'  placeholder='กก-1234 กรุงเทพมหานคร'>"
	end select
	
	inputTag = strx
	
end Function

Function inputTagMax(fname,ftype, fvalue, fsize, maxSize)
	dim strx
	dim valx

	valx = getvalue(fname)
	if len(valx)=0  then
	   valx = fvalue
	end if
	if not isnull(valx) then
	   valx = replace(valx,"'","&#39;")
	   valx = replace(valx,"""", "&#34;")
	end if

	select case ftype
	case "T"
	    strx ="<input type = text id='" & fname & "'  name='" & fname & "'  size=" &  fsize  & "  value='" & valx & "'>"
    case "D"
        strx ="<input type = text id='" & fname & "' name='" & fname & "'  size=" &  fsize  & " maxlength=10  value='" & valx & "' title='ใส่เป็น DDMMYY ปีคศ' onblur='check_date(this)' >"
	case "N"
	    strx ="<input type = text id='" & fname & "' name='" & fname & "'  size=" &  fsize  & " maxlength=" &  maxSize  & "  value='" & valx & "'  onblur='check_number(this)' >"
	end select
	inputTagMax = strx

end Function

function showlockNo1(fname,fvalue,fsize)
		 showlockNo1 = " <input type = text size=" & fsize &  " style='text-align:right;color:gray;' id ="  &  fname & " name ="  &  fname & " value=" & formatnumber(fvalue ,2) & ">"

end function

sub buildAddress

	 dim tmpx
	 dim lx, i, cx

	 addZip =""
	 addLine1 =""
	 addLine2 =""
	 addDistrict =""
	 addProvince =""

	 if len(addtemp) > 10 and not isnull(addtemp) then
		 tmpx = trim(addtemp)

		 if isnumeric(right(tmpx,5)) then
		 	addzip = right(tmpx,5)
			tmpx = mid(tmpx,1,len(tmpx) -5)
		 end if

		 '- get addLine 2
		 '
		 '
		  lx = InStr(1, tmpx, "อ.")
		  cx =2
		  If lx = 0 Then
		  	 lx = InStr(1, tmpx, "อำเภอ")
		  	 cx =5
		  	 If lx = 0 Then
		  		lx = InStr(1, tmpx, "เขต")
		  		cx=3
		  	 end if
		  end if
		  if lx > 0 then
   	         addLine1 = Trim(MID(tmpx, 1, lx - 1))
		     addline2 = trim(mid(tmpx,lx+cx))
		  end if

		  for i =  len(addline2) to 1 step -1

		      if mid(addline2,i,1) = " " then

		         addProvince = trim(mid(addline2,i))
		         addDistrict = trim(mid(addline2,1,i-1))
		         addline2 =""
				 exit for
		      end if
		  next

		  if len(addprovince) > 2 then
			  if mid(addProvince,1,"2") = "จ." then
				 addprovince = trim(mid(addprovince,3))
			  end if
			  if instr(1,addprovince, "กทม") > 0 then addprovince = "กรุงเทพ"
			  addprovince = matchstrcmb("cmbprovince",addprovince)
		  end if

		  if len(addLine1) > 15 then

				  lx = InStr(1, addLine1, "ต.")

				  If lx = 0 Then
					 lx = InStr(1, addLine1, "ตำบล")
					 If lx = 0 Then
						lx = InStr(1, addLine1, "แขวง")
					 end if
				  end if

				  if lx > 0 then
					 addline2 = trim(mid(addLine1,lx))
					 addLine1 = Trim(MID(addLine1, 1, lx - 1))

				  end if

		  end if

	 end if

end sub

function matchstrcmb(cmb, str)
         dim i,x
         i = instr(1,application(cmb),str)
         if i > 0 then
            for  x = i to 1 step -1
                 if mid(application(cmb),x,1) ="=" then
                    matchstrcmb = mid(application(cmb), x+1, i-x-2)
                    exit function
                 end if
            next

         end if
end function

function getchecked(v1,v2)
	if v1 = v2 then
	   getchecked ="checked"
	else
	   getchecked =""
	end if
end function

Function URLDecode(sConvert)
    Dim aSplit
    Dim sOutput
    Dim I
    If IsNull(sConvert) Then
       URLDecode = ""
       Exit Function
    End If

    ' convert all pluses to spaces
    sOutput = REPLACE(sConvert, "+", " ")

    ' next convert %hexdigits to the character
    aSplit = Split(sOutput, "%")

    If IsArray(aSplit) Then
      sOutput = aSplit(0)
      For I = 0 to UBound(aSplit) - 1
        sOutput = sOutput & _
          Chr("&H" & Left(aSplit(i + 1), 2)) &_
          Right(aSplit(i + 1), Len(aSplit(i + 1)) - 2)
      Next
    End If

    URLDecode = sOutput
End Function

Function getExpirydateErr( dFrom, dTo)

    If Month(dFrom) = Month(dTo)  And Day(dFrom) = Day(dTo) and Year(dFrom)+1 = Year(dTo) Then
        getExpirydateErr =0
    Else
        getExpirydateErr = 1
    End If

End Function

sub cmdVerify
	call cmdCallupdate("V")
end sub

sub cmdSave
    call cmdCallupdate("S")
end sub

sub cmdCallupdate(stype)
dim strxx ,str2
dim rs, sql, objrst
dim stypeforce ,cardrelation

	objconn.execute "delete from  TQMSALE.staffsubmititem where staffid = " & session("staffid")

		 call submitpack("paymentmode",request("pmode"))
		 call submitpack("leadid",session("leadid"))
		 call submitpack("leadassignid",session("leadassignid"))
		 
		 call submitpack("url", request.ServerVariables("URL"))
		 call submitpack("SOURCE","")

		 call submitpack("leadprefix","")
		 call submitpack("leadname","")
		 call submitpack("leadsurname","")
		 call submitpack("corpstatus","")

		 call submitpack("SALETYPE","")
		 call submitpack("EXTENDSALEIDINT","")
		 call submitpack("EXTENDSALEID","")
		 call submitpack("salepiority","")
		 
		 call submitpack("customercitizenid","")

		 if request("cmbrace") <> 0 then
			call submitpack("RACE","ไทย")
		 else
			call submitpack("RACE","")
		 end if
		 
		 if request("cmbnation") <> 0 then
			call submitpack("NATIONALITY","ไทย")
		 else
			call submitpack("NATIONALITY","")
		 end if
		
		 call submitpack("birthdate","")
		 call submitpack("gender","")
		 call submitpack("martialstatus","")
		 call submitpack("child" ,"")

		 call submitpack("phonemobile","")
		 call submitpack("phonehome","")
		 call submitpack("phoneoffice","")

		 call submitpack("leadphonehome" , "" )
		 call submitpack("leadphonemobile" , "")
		 call submitpack("leadphoneoffice" , "" )
		 
		 call submitpack("email","")
		 call submitpack("phonefax","")
		 
		 call submitpack("company","")
		 call submitpack("jobtitle","")
		 call submitpackno("income","")
		 call submitpack("handicap","")
		 call submitpack("occupationid","")
		 call submitpack("jobname","")
		 call submitpack("REF_STAFFCODE" ,request("refstaffcode"))
		 
		 'ที่อยู่------------------------
		 call submitpack("homeadd1" ,"")
		 call submitpack("homeadd2" ,"")
		 call submitpack("homedis" ,"")
		 call submitpack("homeprovince" ,"")
		 call submitpack("homezipcode" ,"")
		 call submitpack("homeno" ,"")
		 call submitpack("homegroup" ,"")
		 call submitpack("homebuilding" ,"")
		 call submitpack("homebystreet" ,"")
		 call submitpack("homepath" ,"")
		 call submitpack("homestreet" ,"")
		 call submitpack("homearea" ,"")
		 call submitpack("homeprefix" ,"")
		 call submitpack("homename" ,"")
		 call submitpack("homesurname" ,"")
		 call submitpack("homecontact" ,"")
		 call submitpack("homeaddphone" ,"")
		 call submitpack("homedistricttype" ,"")
		 call submitpack("homerouteid" ,"")

		 call submitpack("offadd1" ,"")
		 call submitpack("offadd2" ,"")
		 call submitpack("offdis" ,"")
		 call submitpack("offprovince" ,"")
		 call submitpack("offzipcode" ,"")
		 call submitpack("offno" ,"")
		 call submitpack("offgroup" ,"")
		 call submitpack("offbuilding" ,"")
		 call submitpack("offbystreet" ,"")
		 call submitpack("offpath" ,"")
		 call submitpack("offstreet" ,"")
		 call submitpack("offarea" ,"")
		 call submitpack("offprefix" ,"")
		 call submitpack("offname" ,"")
		 call submitpack("offsurname" ,"")
		 call submitpack("offcontact" ,"")
		 call submitpack("offaddphone" ,"")
		 call submitpack("offdistricttype" ,"")
		 call submitpack("offrouteid" ,"")

		 call submitpack("contactadd1" ,"")
		 call submitpack("contactadd2" ,"")
		 call submitpack("contactdis" ,"")
		 call submitpack("contactprovince" ,"")
		 call submitpack("contactzipcode" ,"")
		 call submitpack("contactno" ,"")
		 call submitpack("contactgroup" ,"")
		 call submitpack("contactbuilding" ,"")
		 call submitpack("contactbystreet" ,"")
		 call submitpack("contactpath" ,"")
		 call submitpack("contactstreet" ,"")
		 call submitpack("contactarea" ,"")
		 call submitpack("contactprefix" ,"")
		 call submitpack("contactname" ,"")
		 call submitpack("contactsurname" ,"")
		 call submitpack("contactcontact" ,"")
		 call submitpack("contactaddphone" ,"")
		 call submitpack("contactdistricttype" ,"")
		 call submitpack("contactrouteid" ,"")
		 
		 call submitpack("contactaddresstype","")
		 call submitpack("postaddresstype","")
		 call submitpack("policyaddresstype","")
		 call submitpack("taxaddresstype","")
		 
		 call submitpack("routeid","")
		 call submitpack("deliverydate","")
		 call submitpack("remarkadmin","")
		 call submitpack("contactperson","")
		 
		 '1# start
		call submitpack("flgtax" ,request("flgtax"))
		if request("flgtax") = "1" then
			call submitpackno("taxrate" ,request("taxrate"))
		else
			call submitpackno("taxrate" ,0)
		end if
		 '1# end
		 
		 ' ข้อมูลผู้เอาประกัน---------------
		 call submitpack("insureprefix","")
		 call submitpack("insurename","")
		 call submitpack("insuresurname","")
		 'call submitpack("insuretype","")
		 call submitpack("citizenid","")		 
		 call submitpack("insurebirthdate","")
		 
		 'ข้อมูลกรมธรรน์-----------------
		 call submitpack("suppliercode","")
		 call submitpack("productid","")
		 call submitpack("supplierdate","")
		 call submitpack("policydate","")
		 call submitpack("expirydate","")
		 call submitpackno("cover","")
		 
		 'ข้อมูลเพิ่มเติม-----------------
		 call submitpackno("coverv3rd","")
		 call submitpackno("coverv3rdasset","")
		 call submitpackno("coverv3rdtime","")
		 call submitpack("driver1name","")
		 call submitpack("driver1licenseid","")
		 
		 'มูลค่ากรมธรรน์-----------------
		  call submitpackno("totalamount" ,"" )
		  call submitpackno("amount","")
		  call submitpackno("duty","")
		  call submitpackno("vat","")
		  call submitpackno("netvalue","")
		  call submitpackno("extdissale","")
		  call submitpackno("extdiscom","")
		  call submitpackno("extdiscom_percent","")
		  call submitpackno("discountother_percent","")
		  call submitpackno("netamount","")
		  call submitpackno("balance","")
		  call submitpackno("taxvalue","")

		 'รายละเอียดลักษณะอาคาร-----------------
		 call submitpack("housecodeno","") 
		 call submitpack("ownerstatus","") 
		 call submitpack("riskcode","") 
		 call submitpack("constcode","") 
		 call submitpack("floorcount","") 
		 call submitpack("unitcount","") 
		 call submitpack("assetwidth","") 
		 call submitpack("assetlength","") 
		 call submitpack("assetarea","") 
		 call submitpackno("buildingamount","") 
		 call submitpackno("contentamount","") 
		 call submitpack("contentremark","") 
		 
		 'ลักษณะที่อยู่---------------------------
		 call submitpack("homestatus","") 
		 call submitpackno("ownerpay","") 
		 call submitpackno("renterpay","") 
		 call submitpack("home_year","") 
		 call submitpack("home_month","") 
		 
		 'ข้อมูลอาชีพ---------------------------
		 call submitpack("graduation","") 
		 call submitpack("businesstype","") 
		 call submitpack("businesscategory","")
		 call submitpack("position","")
		 call submitpack("experience","")
		 call submitpackno("ownerincome","")
		 call submitpackno("freelanceincome","")
		 call submitpackno("otherincome","")
		 call submitpack("salaryrange","")
		 
		 'ข้อมูลป้ายทะเบียน------------------------
		 call submitpack("countplateid" , countplateid)
		 for i = 1 to countplateid
			if i < 10 and i <> "" then
				call submitpack("plateid_00"& i ,"") 
			elseif i >= 10 and i < 100  Then
				call submitpack("plateid_0"& i ,"") 
			else 
				call submitpack("plateid_"& i ,"") 
			end if 
		 next
		 
		 'ข้อมูลผู้รับผลประโยชน์------------
		 call submitpack("nominee","")
		 call submitpack("relationcode","")
		 call submitpack("relationtext","")
		 call submitpack("nomineebirthdate","")
		 call submitpack("nominee2","")
		 call submitpack("relationcode2","")
		 call submitpack("relationtext2","")
		 call submitpack("nomineebirthdate2","")
		 
		 call submitpack("cardtype","")
		 call submitpack("cardbankid","")
		 call submitpack("cardnumber","")
		 call submitpack("cardexpiry1","")
		 call submitpack("cardexpiry2","")
		 call submitpack("bankid","")
		 
		 if request("cardholdername")  <> "" then
			if Cstr(request("cardholderrelation")) = "อื่นๆ" then 
				cardrelation = request("cardholderrelation_txt")
			else
				cardrelation = request("cardholderrelation")
			end if 

			if cardrelation <> "" then
				call submitpack("cardholdername" ,request("cardholdername") & "(" & cardrelation & ")")	
			else	
				call submitpack("cardholdername","")		
			end if
		 else	
			call submitpack("cardholdername","")	
		 end if
	
		 call submitpack("bankbranch","")
		 call submitpack("bankaccountname","")
		 call submitpack("bankaccounttype","")
		 call submitpack("bankaccount","")
		 call submitpack("remark","")
		 
		 'การชำระเงิน---------------------
		 call submitpack("installment" , installment)
		 for i = 1 to installment
			call submitpack("INSTALLTYPE" & i,"" )
			call submitpack("INSTALLDATE" & i,"" )
			call submitpack("INSTALLAMOUNT" & i,"" )
			call submitpack("RECEIVECOST" & i,"")
		 next
		 
		 ' if request("newamount") <> "" then call submitpack("newamount","")

		' if cint(request("coveritemtotal")) > 0 then
		   ' call submitpack("coveritemtotal","")
		   ' for i = 0 to cint(request("coveritemtotal")) -1
		       ' call submitpack("cover_" & i,"")
		       ' call submitpack("premium_" & i,"")
		       ' call submitpack("covercode_" & i,"")

		   ' next

		' end if


 	stypeforce = stype
    'strxx =now()

	set rs = objconn.execute("select submitstr from TQMSALE.staffsubmit where staffid =" & session("saleid"))
	if rs.eof then
	   stypeforce = "V"
	else
	   if rs("submitstr") <> strxx then
	      stypeforce = "V"
	   end if

	end if
	
	rs.close

	on error resume next

	'response.write len(str2) & "_" & len(strxx)

	objconn.execute "delete from TQMSALE.staffsubmit where staffid =" & session("saleid")
	objconn.execute "insert into TQMSALE.staffsubmit (staffid,submittype ) " & _
													  " values(" & session("saleid") & ",'" & stypeforce & "')"

    if  objConn.Errors.count > 0 then
	    submiterror=  "<div class=hsmall width=640 align=left>" & _
	    			  "<font color=red><b>*** ERROR:Error จากเงื่อนไขฐานข้อมูล ( Submit phase )  ***</b></font><br>" &  _
	    			  "<font color=blue>" & odbcerr( objConn.Errors(0) ) & "</div>"

	else

		set rs = objconn.execute("select errorstr from TQMSALE.staffsubmit where staffid =" & session("saleid"))
		if not rs.eof then
		   submiterror=rs("errorstr")
		   if isnull(submiterror) and stypeforce <> stype then
			  submiterror = " "
		   end if

		else
		    'submiterror=submiterror & " ERROR:Fail to conect database "
		   if  objConn.Errors.count > 0 then
		   	   submiterror= "<div class=hsmall width=640 align=left>" & _
		   				    "<font color=red><b>*** ERROR:Error จากเงื่อนไขฐานข้อมูล ( Read back phase ) ***</b></font><br>" &  _
		     			    "<font color=blue>" & odbcerr( objConn.Errors(0) ) & "</div>"
		   end if

		end if
	end if

end sub

function odbcerr( errin )
		 dim tmpstr
         tmpstr = errin
         'tmpstr = replace(tmpstr,"[Oracle][ODBC][Ora]ORA-20001:","<br>")
         tmpstr = replace(tmpstr,"ORA-","<br>ORA-")
		 odbcerr = tmpstr
end function

sub submitpack(nx, vx)
	    dim strx
		if vx ="" then
		   strx = replace(request(nx),"'","''")
		else
		   strx = vx
		end if
		'response.write "staffid : " & session("saleid") & " | Submit code : " & ucase(nx) & " | submitvalue : " & strx & "<br>"
		objconn.execute " insert into TQMSALE.staffsubmititem(staffid,submitcode, submitvalue ) " & _
					   " values(" & session("saleid") & ",'"  & ucase(nx) & "','" & strx& "')"

end sub

sub submitpackno(nx, vx)
	    dim strx

		if vx ="" then
		   strx = Replace(replace(request(nx),"'","''"),",","")
		else
		   strx = vx
		end if
		objconn.execute "insert into TQMSALE.staffsubmititem(staffid,submitcode, submitvalue ) " & _
					   " values(" & session("saleid") & ",'"  & ucase(nx) & "','" & strx& "')"
end sub

function getPolicyDate
		 dim rs
		 set rs = objconn.execute( "select TQMSALE.CHECKHOLIDAY(trunc(sysdate) +1,'Y' ) pDATE from dual ")
		 getPolicyDate = formatDateEdit(rs(0))

end function

function getPolicyDateRenew

		getPolicyDateRenew = rslead("policydate")

end function

sub DisplayProductQuestion

	dim SQL, objrst
	dim tmpstr1, tmpstr2, ext1,ext2, ext3, ext4
	sql = "select * from TQMSALE.productquestion where questioncode ='" & showquestion & "' order by questionno,choiceno,lineno"
	set objrst = objConn.Execute(sql)

    response.write "<table width=720 ><tr>"
   	dim strextsale
   	strextsale = ""
   	if rslead("saletype") = "R" and supplierid = "341" then
   		strextsale = rslead("extsaleid")
   	end if
    if strextsale <> ""  then
		response.write "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
		response.write "<a href='javascript:getquestion(" & strextsale & ");'><img src=images/question.jpg border=0>คัดลอกข้อมูลคำถามสุขภาพจากปีที่แล้ว</a>"
	end if
	'--------
    response.write "</td></tr><tr><td width=5></td><td bgcolor=#404040>"
	response.write "<table width=800 bgcolor=#FFFFF7>"

	do  until objrst.eof
	  	if cint(objrst("choiceno")) = 0 then

	  	  response.write "<tr valign=top>" & _
	  	  			     "<td align=right width=20>" &  objrst("questionno")  & "</td>" & _
	  	  			     "<td width=780>" & objrst("questiontext") & "</td>" & _
	  	  			     "</tr>"
	  	else

	  	  select case objrst("questiontype")
	  	  case "T"
	  	  	  response.write "<tr>" & _
	  	  	  			     "<td></td>" & _
	  	  	  			     "<td align=left >" & getextTag(objrst("questiontype"), "Q_" & objrst("questionno"), objrst("questionno"), objrst("choiceno") , cstr(objrst("choiceno")),objrst("questiontext") ) & "</td>" & _
	  	  	  			     "</tr>"
	  	  case "O"
	  	  	  response.write "<tr>" & _
	  	  	  			     "<td></td>" & _
	  	  	  			     "<td align=left>" & getextTag(objrst("questiontype"), "Q_" & objrst("questionno"), objrst("questionno"), objrst("choiceno") , cstr(objrst("choiceno")),objrst("questiontext") ) & "" & _
	  	  	  			     "<font color=#0000F0>" & _
	  	  	  			     " &nbsp;&nbsp;" & getextTag(objrst("EXTQUESTIONTYPE1"), "Q_EX1" & objrst("questionno") & "_" & objrst("choiceno"), objrst("questionno"), objrst("choiceno") , cstr(objrst("choiceno")),objrst("extquestion1")  )   & "" & _
	  	  	  			     " &nbsp;" & getextTag(objrst("EXTQUESTIONTYPE2"), "Q_EX2" & objrst("questionno") & "_" & objrst("choiceno"), objrst("questionno"), objrst("choiceno") , cstr(objrst("choiceno")) ,objrst("extquestion2") )   & "" & _
	  	  	  			     " &nbsp;" & getextTag(objrst("EXTQUESTIONTYPE3"), "Q_EX3" & objrst("questionno") & "_" & objrst("choiceno"), objrst("questionno"), objrst("choiceno") , cstr(objrst("choiceno")),objrst("extquestion3")  )   & "" & _
	  	  	  			     " &nbsp;" & getextTag(objrst("EXTQUESTIONTYPE4"), "Q_EX4" & objrst("questionno") & "_" & objrst("choiceno"), objrst("questionno"), objrst("choiceno") , cstr(objrst("choiceno")),objrst("extquestion4")  )   & "</td>" & _
	  	  	  			     "</tr>"

	  	  case "C"
	  	      response.write "<tr><td>" & objrst("questiontext") & "</td></tr>"
	  	  end select

	  	end if

	    objrst.movenext
	loop

	response.write "</table>"
	response.write "</td></tr>" & _
				    "<tr><td colspan=2></td></tr>" & _
	                "</table>"

end sub

function getextTag(qtype, qname, qno,cno, setval, qtext )
		 dim strx
		 select case qtype
		 case "T"
		 	   strx = qtext & " <input type=text name = " & qname & " size=30 value='" & request(qname) &  "'>"
		 case "O"
		      strx =  "<input type=radio name=" & Qname  & " value=" & setval & " " & getchecked(setval,request(qname)) & "> " & qtext
		 case "C"
			  strx = "<input type=checkbox name=" & Qname  & " "  & getChecked(request(qname),"-1") & " value=-1>  " & qtext
		 end select
		 getextTag = strx

end function

function getComboNation(cmbName, SetValue)

		if SetValue = 1 or SetValue = "" then
	     getComboNation = "<select name=" & cmbName & " id="& cmbName &"><option value=1 selected>ไทย</option><option value=0>อื่น ๆ</option></select>"
		else
	     getComboNation = "<select name=" & cmbName & " id="& cmbName &"><option value=1>ไทย</option><option value=0 selected>อื่น ๆ</option></select>"
		end if

end Function

function getComboSession(cmbName, RecordSource, SetValue)
	dim strx
	dim x
	x=getValue(cmbname)
	if x="" then
	   x = setvalue
	end if

	strx= Replace(session(RecordSource),"sysvalue",cmbname & " id=" & cmbname)
	if x <> "" then
	   strx = Replace(strx,"value=" & x, "value=" & x & " selected")
	end if
	getComboSession = strx

end Function

' function getComboReligion(cmbName, SetValue)

		' select case SetValue
		' case "",1
			' getComboReligion = "<select name=" & cmbName & ">" &_
							   ' "<option value=1 selected>พุทธ</option>" &_
							   ' "<option value=2>คริสต์</option>" &_
							   ' "<option value=3>อิสลาม</option>" &_
							   ' "<option value=0>อื่น ๆ</option>" &_
							   ' "</select>"
		' case 2
			' getComboReligion = "<select name=" & cmbName & ">" &_
							   ' "<option value=1>พุทธ</option>" &_
							   ' "<option value=2 selected>คริสต์</option>" &_
							   ' "<option value=3>อิสลาม</option>" &_
							   ' "<option value=0>อื่น ๆ</option>" &_
							   ' "</select>"
		' case 3
			' getComboReligion = "<select name=" & cmbName & ">" &_
							   ' "<option value=1>พุทธ</option>" &_
							   ' "<option value=2>คริสต์</option>" &_
							   ' "<option value=3 selected>อิสลาม</option>" &_
							   ' "<option value=0>อื่น ๆ</option>" &_
							   ' "</select>"
		' case 0
			' getComboReligion = "<select name=" & cmbName & ">" &_
							   ' "<option value=1>พุทธ</option>" &_
							   ' "<option value=2>คริสต์</option>" &_
							   ' "<option value=3>อิสลาม</option>" &_
							   ' "<option value=0 selected>อื่น ๆ</option>" &_
							   ' "</select>"
		' end select

' end Function

function getComboTXT(cmbName, RecordSource, SetValue)

		 dim strx
		 dim x
		 x=getValue(cmbname)
		 if x="" then
		    x = setvalue
		 end if

	     strx= Replace(application(RecordSource),"sysvalue",cmbname & " id=" & cmbname)
	     if x <> "" then
	        strx = Replace(strx,"value='" & x & "'" , "value='" & x & "' selected")
	     end if
	     getComboTXT = strx

end Function

function getCombo(cmbName, RecordSource, SetValue)

		 dim strx
		 dim x
		 x=getValue(cmbname)
		 if x="" then
		    x = setvalue
		 end if

	     strx= Replace(application(RecordSource),"sysvalue",cmbname & " id=" & cmbname )
	     if x <> "" then
	        strx = Replace(strx,"value=" & x, "value=" & x & " selected")
	     end if
	     getCombo = strx

end Function

Function inputTagNoCal(fname,ftype, fvalue, fsize)
	dim strx
	dim valx

	valx = getvalue(fname)
	if len(valx)= 0  then
	   valx = fvalue
	end if

	if not isnull(valx) then
	   valx = replace(valx,"'","&#39;")
	   valx = replace(valx,"""", "&#34;")
	end if

	select case ftype
		case "N"
			'strx ="<input type = text id='" & fname & "' name='" & fname & "'  size=" &  fsize  & "   value='" & formatnumber(cNum(valx),2) & "'  onblur='check_number_cal(this)'  style='text-align:right;'  >"
			strx ="<input type = text id='" & fname & "' name='" & fname & "'  size=" &  fsize  & "   value='" & formatnumber(cNum(valx),2) & "'  onchange=""check_number_cal('" & fname & "')"" class='inputsalevalue' style='text-align:right;'>"
		case "R"
			strx ="<input type = text id='" & fname & "' name='" & fname & "'  size=" &  fsize  & "   value='" & formatnumber(cNum(valx),2) & "'  class='inputsalevalue' style='text-align:right;background-color:#E2E0E0;' readonly/>" 
	end select
	inputTagNoCal = strx

end Function
%>

<br><br>
<iframe src = sysautorefresh.asp width=200 height=20 frameborder=0 scrolling=no></iframe>
<script src="include/jquery/jquery-2.1.1.min.js" type="text/javascript"></script>
<link href="include/jquery/select2.min.css" rel="stylesheet" />
<script src="include/jquery/select2.min.js"></script>
<script>

$(document).ready(function(){
	if('<%=getleaddata("refstaff")%>' == "1"){
		document.getElementById("refstaffcode").disabled = false;
	}else{
		document.getElementById("refstaffcode").disabled = true;
	}
	ChkDiscountCom();
	openDetail(event,'<%=request("chk_oDetail")%>');	
	
});

//1# start
/*
$('#taxrate').keydown(function(event){ 
    var v = parseFloat(this.value + String.fromCharCode(event.which));
    return parseFloat(v).between(0,1,true);
});*/

function gettaxrate(e){
	//if(document.getElementById("corpstatus").value == 'Y'){
		if(e == 1){
			document.getElementById("taxrate").value = 1
			document.getElementById("txttaxrate").value = 1
			
		}else{
			document.getElementById("taxrate").value = 0
			document.getElementById("txttaxrate").value = 0
		}		
	/*}else{
		document.getElementById("taxrate").value = 0;
	}*/
}
//1# end
	
function getCitizenDummy(input){
	document.getElementById(input).value = '1111111111119';
}
 
function openDetail(evt,detail){
	document.getElementById("chk_oDetail").value = detail; 
	var i, tabcontent, tablinks;
	  tabcontent = document.getElementsByClassName("tabcontent");
	  for (i = 0; i < tabcontent.length; i++) {
		tabcontent[i].style.display = "none";
	  }
	  tablinks = document.getElementsByClassName("tablinks");
	  for (i = 0; i < tablinks.length; i++) {
		tablinks[i].className = tablinks[i].className.replace(" active", "");
	  }

	  if (detail)
	  {
		document.getElementById(detail).style.display = "block";
	  }evt.currentTarget.className += " active";
	  
}
	
function gethashtag(src){
	var url="hashtag.asp?src=" + src;
    window.open(url, "ROUTECODE", "width=550,height=300,top=100,right=100,resizable=yes,scrollbars=yes");
}

function ChkDiscountCom(){
	var Disc_team = <%=DiscountComTeam%>;
	var Disc_staff = <%=DiscountComStaff%>;

	if(Disc_team > 0){
		document.getElementById("extdiscom").value = "";
		document.getElementById("extdiscom").readOnly = true;
		document.getElementById("extdiscom").style.background = "#E2E0E0";
		
		if(Disc_staff > 0){
			document.getElementById("extdiscom").value = "";
			document.getElementById("extdiscom").readOnly = false;
			document.getElementById("extdiscom").style.background = "";
		}
	}
}

	function prefixhandler(corptype) {
		// Get the prefix data from Application("strprefix")
		var strPrefix = '<%=Application("strprefix")%>';
		
		// Get all prefix dropdown elements
		var prefixDropdowns = [
			document.getElementById("leadprefix"),
			document.getElementById("homeprefix"),
			document.getElementById("offprefix"),
			document.getElementById("contactprefix"),
			document.getElementById("insureprefix")
		];
		
		// Process each dropdown - clear existing options using removeAllOptions function
		for (var d = 0; d < prefixDropdowns.length; d++) {
			var dropdown = prefixDropdowns[d];
			if (!dropdown) continue; // Skip if dropdown doesn't exist
			
			// Clear existing options using removeAllOptions function
			removeAllOptions(dropdown);
			
			// Add default empty option
			addOption(dropdown, '', '** กรุณาเลือก **');
		}
		
		// Split the prefix string by "|" to get individual prefix entries
		var prefixEntries = strPrefix.split("|");
		
		// Loop through each prefix entry
		for (var i = 0; i < prefixEntries.length; i++) {
			if (prefixEntries[i].trim() !== "") {
				// Split each entry by "*" to get prefix and type
				var prefixData = prefixEntries[i].split("*");
				
				// Check if the entry has both prefix and type
				if (prefixData.length >= 2) {
					var prefix = prefixData[0];
					var type = prefixData[1];
					
					// Add the prefix to all dropdowns if it matches the corptype using addOption function
					if (type === corptype) {
						for (var d = 0; d < prefixDropdowns.length; d++) {
							var dropdown = prefixDropdowns[d];
							if (!dropdown) continue; // Skip if dropdown doesn't exist
							
							// Add option using addOption function
							addOption(dropdown, prefix, prefix);
						}
					}
				}
			}
		}
	}
	
	function Copytaxrate(v){
		document.getElementById("txttaxrate").value = v;
	}
</script>
<!--#include file=include\footer.inc -->
<!--#include file=include\function.inc -->

<!--
History
1#	chadaporn.pia	13/03/2025	BR250107 ปรับ Data Field Status non-Motor Edit W (Freeform)
-->