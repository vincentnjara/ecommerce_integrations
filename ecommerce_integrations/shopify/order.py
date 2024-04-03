import json
from typing import Literal, Optional

import frappe
from frappe import _
from frappe.utils import cint, cstr, flt, get_datetime, getdate, nowdate
from shopify.collection import PaginatedIterator
from shopify.resources import Order 
import requests

from ecommerce_integrations.shopify.connection import temp_shopify_session
from ecommerce_integrations.shopify.constants import (
	CUSTOMER_ID_FIELD,
	EVENT_MAPPER,
	ORDER_ID_FIELD,
	ORDER_ITEM_DISCOUNT_FIELD,
	ORDER_NUMBER_FIELD,
	ORDER_STATUS_FIELD,
	SETTING_DOCTYPE,
)
from ecommerce_integrations.shopify.customer import ShopifyCustomer
from ecommerce_integrations.shopify.product import create_items_if_not_exist, get_item_code
from ecommerce_integrations.shopify.utils import create_shopify_log
from ecommerce_integrations.utils.price_list import get_dummy_price_list
from ecommerce_integrations.utils.taxation import get_dummy_tax_category

DEFAULT_TAX_FIELDS = {
	"sales_tax": "default_sales_tax_account",
	"shipping": "default_shipping_charges_account",
}


def sync_sales_order(payload, request_id=None):
	order = payload
	frappe.set_user("Administrator")
	frappe.flags.request_id = request_id

	if frappe.db.get_value("Sales Order", filters={ORDER_ID_FIELD: cstr(order["id"])}):
		create_shopify_log(status="Invalid", message="Sales order already exists, not synced")
		return
	try:
		shopify_customer = order.get("customer") if order.get("customer") is not None else {}
		shopify_customer["billing_address"] = order.get("billing_address", "")
		shopify_customer["shipping_address"] = order.get("shipping_address", "")
		customer_id = shopify_customer.get("id")
		if customer_id:
			customer = ShopifyCustomer(customer_id=customer_id)
			if not customer.is_synced():
				customer.sync_customer(customer=shopify_customer)
			else:
				customer.update_existing_addresses(shopify_customer)

		create_items_if_not_exist(order)

		setting = frappe.get_doc(SETTING_DOCTYPE)
		create_order(order, setting)
	except Exception as e:
		create_shopify_log(status="Error", exception=e, rollback=True)
	else:
		create_shopify_log(status="Success")


def create_order(order, setting, company=None):
	# local import to avoid circular dependencies
	from ecommerce_integrations.shopify.fulfillment import create_delivery_note
	from ecommerce_integrations.shopify.invoice import create_sales_invoice

	so = create_sales_order(order, setting, company)
	if so:
		if order.get("financial_status") == "paid":
			create_sales_invoice(order, setting, so)

		if order.get("fulfillments"):
			create_delivery_note(order, setting, so)


def create_sales_order(shopify_order, setting, company=None):
	customer = setting.default_customer
	if shopify_order.get("customer", {}):
		if customer_id := shopify_order.get("customer", {}).get("id"):
			customer = frappe.db.get_value("Customer", {CUSTOMER_ID_FIELD: customer_id}, "name")

	so = frappe.db.get_value("Sales Order", {ORDER_ID_FIELD: shopify_order.get("id")}, "name")

	if not so:
		items = get_order_items(
			shopify_order.get("line_items"),
			setting,
			getdate(shopify_order.get("created_at")),
			taxes_inclusive=shopify_order.get("taxes_included"),
		)

		if not items:
			message = (
				"Following items exists in the shopify order but relevant records were"
				" not found in the shopify Product master"
			)
			product_not_exists = []  # TODO: fix missing items
			message += "\n" + ", ".join(product_not_exists)

			create_shopify_log(status="Error", exception=message, rollback=True)

			return ""

		taxes = get_order_taxes(shopify_order, setting, items)
		so = frappe.get_doc(
			{
				"doctype": "Sales Order",
				"naming_series": setting.sales_order_series or "SO-Shopify-",
				ORDER_ID_FIELD: str(shopify_order.get("id")),
				ORDER_NUMBER_FIELD: shopify_order.get("name"),
				"customer": customer,
				"transaction_date": getdate(shopify_order.get("created_at")) or nowdate(),
				"delivery_date": getdate(shopify_order.get("created_at")) or nowdate(),
				"company": setting.company,
				"selling_price_list": get_dummy_price_list(),
				"ignore_pricing_rule": 1,
				"items": items,
				"taxes": taxes,
				"tax_category": get_dummy_tax_category(),
			}
		)

		if company:
			so.update({"company": company, "status": "Draft"})
		so.flags.ignore_mandatory = True
		so.flags.shopiy_order_json = json.dumps(shopify_order)
		so.save(ignore_permissions=True)
		so.submit()

		if shopify_order.get("note"):
			so.add_comment(text=f"Order Note: {shopify_order.get('note')}")

	else:
		so = frappe.get_doc("Sales Order", so)

	return so


def get_order_items(order_items, setting, delivery_date, taxes_inclusive):
	items = []
	all_product_exists = True
	product_not_exists = []

	for shopify_item in order_items:
		if not shopify_item.get("product_exists"):
			all_product_exists = False
			product_not_exists.append(
				{"title": shopify_item.get("title"), ORDER_ID_FIELD: shopify_item.get("id")}
			)
			continue

		if all_product_exists:
			item_code = get_item_code(shopify_item)
			items.append(
				{
					"item_code": item_code,
					"item_name": shopify_item.get("name"),
					"rate": _get_item_price(shopify_item, taxes_inclusive),
					"delivery_date": delivery_date,
					"qty": shopify_item.get("quantity"),
					"stock_uom": shopify_item.get("uom") or "Nos",
					"warehouse": setting.warehouse,
					ORDER_ITEM_DISCOUNT_FIELD: (
						_get_total_discount(shopify_item) / cint(shopify_item.get("quantity"))
					),
				}
			)
		else:
			items = []

	return items


def _get_item_price(line_item, taxes_inclusive: bool) -> float:

	price = flt(line_item.get("price"))
	qty = cint(line_item.get("quantity"))

	# remove line item level discounts
	total_discount = _get_total_discount(line_item)

	if not taxes_inclusive:
		return price - (total_discount / qty)

	total_taxes = 0.0
	for tax in line_item.get("tax_lines"):
		total_taxes += flt(tax.get("price"))

	return price - (total_taxes + total_discount) / qty


def _get_total_discount(line_item) -> float:
	discount_allocations = line_item.get("discount_allocations") or []
	return sum(flt(discount.get("amount")) for discount in discount_allocations)


def get_order_taxes(shopify_order, setting, items):
	taxes = []
	line_items = shopify_order.get("line_items")
	vsaccount=''
	vcenter=''
	for line_item in line_items:
		item_code = get_item_code(line_item)
		vsett=frappe.db.get_value('Vendor Account Mapping', {'parent':'Shopify Setting','vendor':line_item.get("vendor")}, ['shipping_revenue_account','vendor_cost_center'], as_dict=1)
		if not vsett:
			vsett=frappe.db.get_value('Vendor Account Mapping', {'parent':'Shopify Setting','vendor':['is', 'null']}, ['shipping_revenue_account','vendor_cost_center'], as_dict=1)
		
		if vsett:
			vsaccount=vsett.shipping_revenue_account
			vcenter=vsett.vendor_cost_center
		for tax in line_item.get("tax_lines"):
			taxes.append(
				{
					"charge_type": "Actual",
					"account_head": get_tax_account_head(tax, charge_type="sales_tax"),
					"description": (
						get_tax_account_description(tax) or f"{tax.get('title')} - {tax.get('rate') * 100.0:.2f}%"
					),
					"tax_amount": tax.get("price"),
					"included_in_print_rate": 0,
					"cost_center": vcenter or setting.cost_center,
					"item_wise_tax_detail": {item_code: [flt(tax.get("rate")) * 100, flt(tax.get("price"))]},
					"dont_recompute_tax": 1,
				}
			)

	update_taxes_with_shipping_lines(
		taxes,
		shopify_order.get("shipping_lines"),
		setting,
		items, vsaccount, vcenter,
		taxes_inclusive=shopify_order.get("taxes_included"),
	)

	if cint(setting.consolidate_taxes):
		taxes = consolidate_order_taxes(taxes)

	for row in taxes:
		tax_detail = row.get("item_wise_tax_detail")
		if isinstance(tax_detail, dict):
			row["item_wise_tax_detail"] = json.dumps(tax_detail)

	return taxes


def consolidate_order_taxes(taxes):
	tax_account_wise_data = {}
	for tax in taxes:
		account_head = tax["account_head"]
		tax_account_wise_data.setdefault(
			account_head,
			{
				"charge_type": "Actual",
				"account_head": account_head,
				"description": tax.get("description"),
				"cost_center": tax.get("cost_center"),
				"included_in_print_rate": 0,
				"dont_recompute_tax": 1,
				"tax_amount": 0,
				"item_wise_tax_detail": {},
			},
		)
		tax_account_wise_data[account_head]["tax_amount"] += flt(tax.get("tax_amount"))
		if tax.get("item_wise_tax_detail"):
			tax_account_wise_data[account_head]["item_wise_tax_detail"].update(tax["item_wise_tax_detail"])

	return tax_account_wise_data.values()


def get_tax_account_head(tax, charge_type: Optional[Literal["shipping", "sales_tax"]] = None):
	tax_title = str(tax.get("title"))

	tax_account = frappe.db.get_value(
		"Shopify Tax Account", {"parent": SETTING_DOCTYPE, "shopify_tax": tax_title}, "tax_account",
	)

	if not tax_account and charge_type:
		tax_account = frappe.db.get_single_value(SETTING_DOCTYPE, DEFAULT_TAX_FIELDS[charge_type])

	if not tax_account:
		frappe.throw(_("Tax Account not specified for Shopify Tax {0}").format(tax.get("title")))

	return tax_account


def get_tax_account_description(tax):
	tax_title = tax.get("title")

	tax_description = frappe.db.get_value(
		"Shopify Tax Account", {"parent": SETTING_DOCTYPE, "shopify_tax": tax_title}, "tax_description",
	)

	return tax_description


def update_taxes_with_shipping_lines(taxes, shipping_lines, setting, items, vsaccount, vcenter, taxes_inclusive=False):
	"""Shipping lines represents the shipping details,
	each such shipping detail consists of a list of tax_lines"""
	shipping_as_item = cint(setting.add_shipping_as_item) and setting.shipping_item
	for shipping_charge in shipping_lines:
		if shipping_charge.get("price"):
			shipping_discounts = shipping_charge.get("discount_allocations") or []
			total_discount = sum(flt(discount.get("amount")) for discount in shipping_discounts)

			shipping_taxes = shipping_charge.get("tax_lines") or []
			total_tax = sum(flt(discount.get("price")) for discount in shipping_taxes)

			shipping_charge_amount = flt(shipping_charge["price"]) - flt(total_discount)
			if bool(taxes_inclusive):
				shipping_charge_amount -= total_tax

			if shipping_as_item:
				items.append(
					{
						"item_code": setting.shipping_item,
						"rate": shipping_charge_amount,
						"delivery_date": items[-1]["delivery_date"] if items else nowdate(),
						"qty": 1,
						"stock_uom": "Nos",
						"warehouse": setting.warehouse,
					}
				)
			else:
				taxes.append(
					{
						"charge_type": "Actual",
						"account_head": get_tax_account_head(shipping_charge, charge_type="shipping"),
						"description": get_tax_account_description(shipping_charge) or shipping_charge["title"],
						"tax_amount": shipping_charge_amount,
						"cost_center": vcenter or setting.cost_center,
					}
				)

		for tax in shipping_charge.get("tax_lines"):
			taxes.append(
				{
					"charge_type": "Actual",
					"account_head": vsaccount or get_tax_account_head(tax, charge_type="sales_tax"),
					"description": (
						get_tax_account_description(tax) or f"{tax.get('title')} - {tax.get('rate') * 100.0:.2f}%"
					),
					"tax_amount": tax["price"],
					"cost_center": vcenter or setting.cost_center,
					"item_wise_tax_detail": {
						setting.shipping_item: [flt(tax.get("rate")) * 100, flt(tax.get("price"))]
					}
					if shipping_as_item
					else {},
					"dont_recompute_tax": 1,
				}
			)


def get_sales_order(order_id):
	"""Get ERPNext sales order using shopify order id."""
	sales_order = frappe.db.get_value("Sales Order", filters={ORDER_ID_FIELD: order_id})
	if sales_order:
		return frappe.get_doc("Sales Order", sales_order)


def cancel_order(payload, request_id=None):
	"""Called by order/cancelled event.

	When shopify order is cancelled there could be many different someone handles it.

	Updates document with custom field showing order status.

	IF sales invoice / delivery notes are not generated against an order, then cancel it.
	"""
	frappe.set_user("Administrator")
	frappe.flags.request_id = request_id

	order = payload

	try:
		order_id = order["id"]
		order_status = order["financial_status"]

		sales_order = get_sales_order(order_id)

		if not sales_order:
			create_shopify_log(status="Invalid", message="Sales Order does not exist")
			return

		sales_invoice = frappe.db.get_value("Sales Invoice", filters={ORDER_ID_FIELD: order_id})
		delivery_notes = frappe.db.get_list("Delivery Note", filters={ORDER_ID_FIELD: order_id})

		if sales_invoice:
			frappe.db.set_value("Sales Invoice", sales_invoice, ORDER_STATUS_FIELD, order_status)

		for dn in delivery_notes:
			frappe.db.set_value("Delivery Note", dn.name, ORDER_STATUS_FIELD, order_status)

		if not sales_invoice and not delivery_notes and sales_order.docstatus == 1:
			sales_order.cancel()
		else:
			frappe.db.set_value("Sales Order", sales_order.name, ORDER_STATUS_FIELD, order_status)

	except Exception as e:
		create_shopify_log(status="Error", exception=e)
	else:
		create_shopify_log(status="Success")


@temp_shopify_session
def sync_old_orders():
	shopify_setting = frappe.get_cached_doc(SETTING_DOCTYPE)
	if not cint(shopify_setting.sync_old_orders):
		return

	orders = _fetch_old_orders(shopify_setting.old_orders_from, shopify_setting.old_orders_to)

	for order in orders:
		log = create_shopify_log(
			method=EVENT_MAPPER["orders/create"], request_data=json.dumps(order), make_new=True
		)
		sync_sales_order(order, request_id=log.name)

	shopify_setting = frappe.get_doc(SETTING_DOCTYPE)
	shopify_setting.sync_old_orders = 0
	shopify_setting.save()


def _fetch_old_orders(from_time, to_time):
	"""Fetch all shopify orders in specified range and return an iterator on fetched orders."""

	from_time = get_datetime(from_time).astimezone().isoformat()
	to_time = get_datetime(to_time).astimezone().isoformat()
	orders_iterator = PaginatedIterator(
		Order.find(created_at_min=from_time, created_at_max=to_time, limit=250)
	)

	for orders in orders_iterator:
		for order in orders:
			# Using generator instead of fetching all at once is better for
			# avoiding rate limits and reducing resource usage.
			yield order.to_dict()

def refund(payload, request_id=None):
	refunds = payload
	frappe.set_user("Administrator")
	frappe.flags.request_id = request_id
	order_id=frappe.db.get_value("Sales Order", filters={ORDER_ID_FIELD: cstr(refunds["order_id"])})
	setting = frappe.get_doc(SETTING_DOCTYPE)
	ermsg=''
	if not order_id:
		create_shopify_log(status="Invalid", message="Sales order Not exists, not synced")
		return
	try:
		
		
		shipd=refunds.get("order_adjustments")
		shipamt=0
		#gateway=refunds.get("transactions")[0].gateway
		
		for ship in shipd:
			if ship.get("reason")=="Shipping refund":
				shipamt+=ship.get("amount") or 0 +ship.get("tax_amount") or 0

		rline_items=refunds.get("refund_line_items")
		order=frappe.get_doc("Sales Order",order_id)
		reitem=[]
		refunditm=[]
		for rlinei in rline_items:
			rline=rlinei.get("line_item")
			tax=rlinei.get('total_tax') or 0
			subtotal=rlinei.get('subtotal') or 0
			product_amt=subtotal-tax
			qty=rline.get("quantity")
			item_code=frappe.db.get_value('Ecommerce Item',{'integration_item_code':rline.get('product_id'),'variant_id':rline.get('variant_id')},'erpnext_item_code')
			reitem.append(item_code)
			refunditm.append({"item_code":item_code,"amt":product_amt,"tax":tax,"qty":qty})
		
		delivary_note=frappe.db.get_value("Delivery Note",{"is_return":0,"shopify_order_id":order.shopify_order_id,"shopify_order_number":order.shopify_order_number},'name')
		delivarynote_doc=frappe.get_doc("Delivery Note",delivary_note)
		delivary_note_doc=frappe.copy_doc(delivarynote_doc)
		items=[]
		taxes=[]
		
		for itm in delivary_note_doc.items:
			if itm.item_code in reitem:
				#item line
				itm.update({
					"name":'',
					"parent":'',
					"amount":itm.amount *-1,
					"base_amount":itm.base_amount *-1,
					"base_net_amount":itm.base_net_amount *-1,
					"net_amount":itm.net_amount *-1,
					"tax_amount":itm.tax_amount *-1,
					"total_amount":itm.total_amount *-1,
					"qty":itm.qty *-1,
					"stock_qty":itm.stock_qty *-1,
					})
				items.append(itm)
		
		subtot=0
		for tx in delivary_note_doc.taxes:			
			if setting.default_sales_tax_account==tx.account_head:
				#tax line
				tax=0
				for itm in refunditm:
					tax+=itm.get('tax')
					subtot+=itm.get('amt')+itm.get('tax')

				tx.update({
					"name":'',
					"parent":'',
					"base_tax_amount": tax*delivary_note_doc.conversion_rate*-1,
					"base_tax_amount_after_discount_amount": tax*delivary_note_doc.conversion_rate*-1,
					"base_total": subtot*-1*delivary_note_doc.conversion_rate,
					"tax_amount": tax*delivary_note_doc.conversion_rate*-1,
					"tax_amount_after_discount_amount": tax*delivary_note_doc.conversion_rate*-1,
					"total": subtot*-1,	
					})
				taxes.append(tx)
			else:
				#shipping line conversion_rate
				
				tx.update({
					"base_tax_amount": shipamt*delivary_note_doc.conversion_rate,
					"base_tax_amount_after_discount_amount": shipamt*delivary_note_doc.conversion_rate,
					"base_total": ((subtot*-1)+shipamt)*delivary_note_doc.conversion_rate,
					"tax_amount": shipamt,
					"tax_amount_after_discount_amount": shipamt,
					"total": (subtot*-1)+shipamt,	
					})
				taxes.append(tx)

	
		
		delivary_note_doc.update({
			"name":'',
			"naming_series": setting.delivery_note_return_series,
			"posting_date": getdate(),
			"posting_time": get_datetime().strftime("%H:%M:%S"),
			"status":"Draft",
			"is_return":1,
			"return_against":delivary_note,
			"items": items,
			"taxes": taxes,
		})
		
		
		#ermsg=str(delivary_note_doc.as_dict())
		dlv = frappe.get_doc(delivary_note_doc)
		dlv.flags.ignore_mandatory = True
		dlv.save(ignore_permissions=True)
		dlv.submit()
		#credit note 
		from erpnext.stock.doctype.delivery_note.delivery_note import make_sales_invoice
		return_invoice = make_sales_invoice(dlv.name)
		return_invoice.shopify_order_id=order.shopify_order_id
		return_invoice.shopify_order_number=order.shopify_order_number
		return_invoice.update({"naming_series":setting.credit_note_series}),
		return_invoice.is_return = True
		#ermsg=str(return_invoice.as_dict())
		return_invoice.save()
		return_invoice.submit()

		from erpnext.accounts.doctype.payment_entry.payment_entry import get_payment_entry
		pentry=get_payment_entry('Sales Invoice',return_invoice.name)
		pentry.reference_no = str(order.shopify_order_number)+" Refund"
		pentry.reference_date = getdate()
		ermsg=str(pentry.as_dict())
		pentry.save()
		pentry.submit()

	except Exception as e:
		create_shopify_log(status="Error", exception=e,message=ermsg, rollback=True)
	else:
		create_shopify_log(status="Success")


@temp_shopify_session
def getall_order_count():
	shopify_setting = frappe.get_cached_doc(SETTING_DOCTYPE)
	
	orders = _fetch_old_orders(shopify_setting.old_orders_from, shopify_setting.old_orders_to)
	odd='order ids: '
	for order in orders:
		odd=odd+str(cstr(order["id"]))
	#order_count=len(orders)
	create_shopify_log(method="getall_order_count", status='Success', message=str(odd))

@temp_shopify_session
def getall_order_custom():
	shopify_setting = frappe.get_doc('Shopify Setting')
	shopify_api_key = shopify_setting.shared_secret
	shopify_api_password = shopify_setting.get_password('password')
	shopify_store_url = shopify_setting.shopify_url

	from_time = get_datetime(shopify_setting.old_orders_from).astimezone().isoformat()
	to_time = get_datetime(shopify_setting.old_orders_to).astimezone().isoformat()
	
	api_endpoint = f"https://{shopify_store_url}/admin/api/2024-01/orders.json"
	headers = {
        'Content-Type': 'application/json',
		"X-Shopify-Access-Token": shopify_api_password
    }
	params = {
		'created_at_min':from_time,
		'created_at_max':to_time,
		'financial_status':'paid',
        'limit': 250,  # Maximum number of orders per page
    }

	orders = []
	while True:
		response = requests.get(api_endpoint, auth=("014c5db001b2b51432e9e8dd4884ed7c", shopify_api_password), headers=headers, params=params)
		data = response.json()
		create_shopify_log(method="getall_order_custom", status='Success', message=str(data))        
		break

@temp_shopify_session
def getall_webhook():
	shopify_setting = frappe.get_doc('Shopify Setting')
	shopify_api_key = shopify_setting.shared_secret
	shopify_api_password = shopify_setting.get_password('password')
	shopify_store_url = shopify_setting.shopify_url
	api_endpoint = f"https://{shopify_store_url}/admin/api/2024-01/webhooks.json"
	headers = {
        'Content-Type': 'application/json',
		"X-Shopify-Access-Token": shopify_api_password
    }
	params = {
		
    }	
		
	response = requests.get(api_endpoint, auth=("014c5db001b2b51432e9e8dd4884ed7c", shopify_api_password), headers=headers, params=params)
	
	print(str(response))