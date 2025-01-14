import frappe
from erpnext.selling.doctype.sales_order.sales_order import make_sales_invoice
from frappe.utils import cint, cstr, getdate, nowdate

from ecommerce_integrations.shopify.constants import (
	ORDER_ID_FIELD,
	ORDER_NUMBER_FIELD,
	SETTING_DOCTYPE,
)
from ecommerce_integrations.shopify.utils import create_shopify_log


def prepare_sales_invoice(payload, request_id=None):
	from ecommerce_integrations.shopify.order import get_sales_order

	order = payload

	frappe.set_user("Administrator")
	setting = frappe.get_doc(SETTING_DOCTYPE)
	frappe.flags.request_id = request_id

	try:
		sales_order = get_sales_order(cstr(order["id"]))
		if sales_order:
			create_sales_invoice(order, setting, sales_order)
			create_shopify_log(status="Success")
		else:
			create_shopify_log(status="Invalid", message="Sales Order not found for syncing sales invoice.")
	except Exception as e:
		create_shopify_log(status="Error", exception=e, rollback=True)


def create_sales_invoice(shopify_order, setting, so):
	if (
		not frappe.db.get_value("Sales Invoice", {ORDER_ID_FIELD: shopify_order.get("id")}, "name")
		and so.docstatus == 1
		and not so.per_billed
		and cint(setting.sync_sales_invoice)
	):

		line_items = shopify_order.get("line_items")
		
		vcenter=''
		for line_item in line_items:
			vsett=frappe.db.get_value('Vendor Account Mapping', {'parent':'Shopify Setting','vendor':line_item.get("vendor")}, ['shipping_revenue_account','vendor_cost_center'], as_dict=1)
			if not vsett:
				vsett=frappe.db.get_value('Vendor Account Mapping', {'parent':'Shopify Setting','vendor':['is', 'null']}, ['shipping_revenue_account','vendor_cost_center'], as_dict=1)
		
		if vsett:
			vcenter=vsett.vendor_cost_center
		cost_center=vcenter or setting.cost_center

		posting_date = getdate(shopify_order.get("created_at")) or nowdate()

		sales_invoice = make_sales_invoice(so.name, ignore_permissions=True)
		sales_invoice.set(ORDER_ID_FIELD, str(shopify_order.get("id")))
		sales_invoice.set(ORDER_NUMBER_FIELD, shopify_order.get("name"))
		sales_invoice.set_posting_time = 1
		sales_invoice.posting_date = posting_date
		sales_invoice.due_date = posting_date
		sales_invoice.naming_series = setting.sales_invoice_series or "SI-Shopify-"
		sales_invoice.flags.ignore_mandatory = True
		#set_cost_center(sales_invoice.items, cost_center)
		set_shipping_account(shopify_order,sales_invoice.items,setting)

		for line_item in line_items:
			if line_item.get("tax_lines",'') and setting.vat_emirate:
				sales_invoice.vat_emirate=setting.vat_emirate

		sales_invoice.insert(ignore_mandatory=True)
		sales_invoice.submit()
		if sales_invoice.grand_total > 0:
			make_payament_entry_against_sales_invoice(sales_invoice, shopify_order, setting, posting_date)

		if shopify_order.get("note"):
			sales_invoice.add_comment(text=f"Order Note: {shopify_order.get('note')}")

def set_shipping_account(shopify_order,items,setting):
	
	cost_center=setting.cost_center
	shipping_charges_account=setting.default_shipping_charges_account
	line_items = shopify_order.get("line_items")
	for line_item in line_items:
		vsett=frappe.db.get_value('Vendor Account Mapping', {'parent':'Shopify Setting','vendor':line_item.get("vendor")}, ['shipping_revenue_account','vendor_cost_center'], as_dict=1)
		if not vsett:
			vsett=frappe.db.get_value('Vendor Account Mapping', {'parent':'Shopify Setting','vendor':['is', 'null']}, ['shipping_revenue_account','vendor_cost_center'], as_dict=1)
	
	if vsett:
		shipping_charges_account=vsett.shipping_revenue_account or shipping_charges_account
		cost_center=vsett.vendor_cost_center or cost_center

	for item in items:
		if setting.shipping_item==item.get('item_code'):
			item.cost_center = cost_center
			item.income_account=shipping_charges_account

def set_cost_center(items, cost_center):
	for item in items:
		item.cost_center = cost_center
		


def make_payament_entry_against_sales_invoice(doc, shopify_order, setting, posting_date=None):
	from erpnext.accounts.doctype.payment_entry.payment_entry import get_payment_entry

	
	if len(shopify_order.get('payment_gateway_names')):
		pa=frappe.db.get_all('Payment Method Accounts',filters={'parent':'Shopify Setting'},fields=['payment_method','account','cost_center'])
		for p in pa:
			if p.payment_method in shopify_order.get('payment_gateway_names'):
				bank_account=p.account or setting.cash_bank_account
				setting.update({'cash_bank_account':bank_account})

	payment_entry = get_payment_entry(doc.doctype, doc.name, bank_account=setting.cash_bank_account)
	payment_entry.flags.ignore_mandatory = True
	payment_entry.reference_no = doc.name
	payment_entry.posting_date = posting_date or nowdate()
	payment_entry.reference_date = posting_date or nowdate()
	payment_entry.insert(ignore_permissions=True)
	payment_entry.submit()