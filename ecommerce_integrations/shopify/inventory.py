from collections import Counter

import frappe
from frappe.utils import cint, create_batch, now
from pyactiveresource.connection import ResourceNotFound
from shopify.resources import InventoryLevel, Variant
import requests

from ecommerce_integrations.controllers.inventory import (
	get_inventory_levels,
	update_inventory_sync_status,
)
from ecommerce_integrations.controllers.scheduling import need_to_run
from ecommerce_integrations.shopify.connection import temp_shopify_session
from ecommerce_integrations.shopify.constants import MODULE_NAME, SETTING_DOCTYPE
from ecommerce_integrations.shopify.utils import create_shopify_log


def update_inventory_on_shopify() -> None:
	"""Upload stock levels from ERPNext to Shopify.

	Called by scheduler on configured interval.
	"""
	setting = frappe.get_doc(SETTING_DOCTYPE)

	if not setting.is_enabled() or not setting.update_erpnext_stock_levels_to_shopify:
		return

	if not need_to_run(SETTING_DOCTYPE, "inventory_sync_frequency", "last_inventory_sync"):
		return

	warehous_map = setting.get_erpnext_to_integration_wh_mapping()
	inventory_levels = get_inventory_levels(tuple(warehous_map.keys()), MODULE_NAME)

	if inventory_levels:
		upload_inventory_data_to_shopify(inventory_levels, warehous_map)


@temp_shopify_session
def upload_inventory_data_to_shopify(inventory_levels, warehous_map) -> None:
	synced_on = now()

	for inventory_sync_batch in create_batch(inventory_levels, 50):
		for d in inventory_sync_batch:
			d.shopify_location_id = warehous_map[d.warehouse]

			try:
				variant = Variant.find(d.variant_id)
				inventory_id = variant.inventory_item_id

				InventoryLevel.set(
					location_id=d.shopify_location_id,
					inventory_item_id=inventory_id,
					# shopify doesn't support fractional quantity
					available=cint(d.actual_qty) - cint(d.reserved_qty),
					cost=d.cost,
				)
				update_inventory_sync_status(d.ecom_item, time=synced_on)
				d.status = "Success"
				if d.cost > 0:
					update_shopify_product_cost(inventory_id, d.cost, d.variant_id)
			except ResourceNotFound:
				# Variant or location is deleted, mark as last synced and ignore.
				update_inventory_sync_status(d.ecom_item, time=synced_on)
				d.status = "Not Found"
			except Exception as e:
				d.status = "Failed"
				d.failure_reason = str(e)

			frappe.db.commit()

		_log_inventory_update_status(inventory_sync_batch)

def update_shopify_product_cost(inventory_id, new_cost,variant_id)-> None:
	setting = frappe.get_doc('Shopify Setting')
	shopify_api_key = setting.shared_secret
	shopify_api_password = setting.get_password('password')
	shopify_store_url = setting.shopify_url
	data = {
		"inventory_item": {
			"cost": new_cost
		}
	}
	api_endpoint = f"https://{shopify_store_url}/admin/api/2024-01/inventory_items/{inventory_id}.json"
	headers = {
		"Content-Type": "application/json",
		"X-Shopify-Access-Token": shopify_api_password
		}
	try:
		response = requests.put(api_endpoint, json=data, headers=headers)
		response.raise_for_status()
		response_data = response.json()
		create_shopify_log(method="update_cost_on_shopify", status='Success', message=str(response_data))
	except requests.exceptions.RequestException as e:
		ermsg=str(api_endpoint)+str(headers)+str(data)+str(e)
		create_shopify_log(method="update_cost_on_shopify", status='Error', message=ermsg)


def _log_inventory_update_status(inventory_levels) -> None:
	"""Create log of inventory update."""
	log_message = "variant_id,location_id,status,failure_reason\n"

	log_message += "\n".join(
		f"{d.variant_id},{d.shopify_location_id},{d.status},{d.failure_reason or ''}"
		for d in inventory_levels
	)

	stats = Counter([d.status for d in inventory_levels])

	percent_successful = stats["Success"] / len(inventory_levels)

	if percent_successful == 0:
		status = "Failed"
	elif percent_successful < 1:
		status = "Partial Success"
	else:
		status = "Success"

	log_message = f"Updated {percent_successful * 100}% items\n\n" + log_message

	create_shopify_log(method="update_inventory_on_shopify", status=status, message=log_message)
