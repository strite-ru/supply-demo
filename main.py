import json
import logging
import math
from typing import List
import pick
from datetime import timedelta, datetime

from rich.console import Console
from rich.table import Table
from strite_data_hub.dataclasses import PredictionFOS, PredictionFOF
from strite_data_hub.parsers.ozon import OzonAPI, OzonStockOnWarehouse, OzonFBOPosting
from strite_data_hub.prediction.supplies.basic import get_basic_predication_supplies_fos, \
    get_basic_predication_supplies_fof
from strite_data_hub.parsers.ozon.utils import get_clusters_with_warehouses, OzonCluster
from tqdm import tqdm

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)
logging.captureWarnings(True)
logger = logging.getLogger(__name__)

# Получение списка кластеров и индентификаторов складов
clusters = get_clusters_with_warehouses()


def init_data() -> OzonAPI:
    client_id = input("Client-Id: ", )
    api_key = input("Api-Key: ")

    return OzonAPI(client_id=int(client_id), key=api_key)


def select_vendor_codes(vendor_codes: List[str]) -> List[str]:
    vendor_codes.sort()
    selected_products = pick.pick(vendor_codes,
                                  "Выберите товары, которые будут отправляться:\nЕсли ничего не выбрано то все товары",
                                  multiselect=True)
    if not selected_products:
        return vendor_codes
    return [vendor_code for vendor_code, _ in selected_products]


def get_cluster_by_region(region: str) -> OzonCluster:
    return next((c for c in clusters if region in c.regions), None)


def get_cluster_by_warehouse_id(warehouse_id: str) -> OzonCluster:
    return next((c for c in clusters if warehouse_id in [w.id for w in c.warehouses]), None)


def get_cluster_by_name(name: str) -> OzonCluster:
    return next((c for c in clusters if c.name == name), None)


def select_cluster_to() -> OzonCluster:
    clusters_names = [c.name for c in clusters]
    selected_cluster = pick.pick(clusters_names,"Выберите кластер для которого необходим расчет")
    return next((c for c in clusters if c.name == selected_cluster[0]), None)


def main(period_transactions: int = 3):
    """
    Получение данных по остаткам и объединение их по кластерам и артикулам
    :param period_transactions: недель до текущей даты, за которые необходимо взять данные по API
    :return: None
    """
    api = init_data()

    postings: List[OzonFBOPosting] = list(OzonFBOPosting.get_postings(api,
                                                                      status="delivered",
                                                                      date_from=(datetime.now() - timedelta(
                                                                          weeks=period_transactions))))

    logger.info(f"Всего отправлений: {len(postings)} за {period_transactions} дней")

    size_supply = int(input("Введите размер поставки (для расчета ФРЗ): "))
    period_supply = int(input("Введите период поставки (для расчета ФПЗ): "))

    orders = []
    for posting in postings:
        cluster_to = get_cluster_by_region(posting.warehouseToRegion)
        if cluster_to is None:
            logger.error(f"Не удалось определить кластер для {posting}")
            continue

        for order in posting.orders:
            # update route if vendor_code, from, to, week exists
            if route := next((r for r in orders if r['vendor_code'] == order.vendor_code and r['to'] == cluster_to and r['week'] == posting.processTo.isocalendar()[1]), None):
                route['quantity'] += order.quantity
                route['price'] += order.price
            else:
                orders.append({
                    "to": cluster_to,
                    "vendor_code": order.vendor_code,
                    "quantity": order.quantity,
                    "price": order.price,
                    "week": posting.processTo.isocalendar()[1]
                })

    print(f"Всего заказов: {len(orders)}")

    # fix price to avg
    for p_data in orders:
        p_data['price'] /= p_data['quantity']

    stocks = list(OzonStockOnWarehouse.get_stocks(api))

    # Конфигурация вывода данных
    console = Console(safe_box=False, force_terminal=True)
    console.size = (250, 38)
    table = Table(show_footer=True)
    table.add_column("Кластер", justify="center")
    table.add_column("Артикул", no_wrap=True)
    table.add_column("Остаток", justify="center")
    table.add_column("Продаж", justify="center")
    table.add_column("ФРЗ дней до", justify="center")
    table.add_column("ФРЗ дата", justify="center")
    table.add_column("ФПЗ размер", justify="center")
    table.add_column("ФПЗ дата", justify="center")

    for cluster in clusters:
        # Find all orders with cluster
        cluster_orders = [o for o in orders if o['to'] == cluster]
        logger.info(f"Для кластера {cluster.name} найдено {len(cluster_orders)} заказов")
        if len(cluster_orders) == 0:
            continue
        # Группировка заказов по артикулам
        vendor_codes = []
        for order in cluster_orders:
            if order['vendor_code'] not in vendor_codes:
                vendor_codes.append(order['vendor_code'])

        for vendor_code in vendor_codes:
            vendor_code_orders = [o for o in cluster_orders if o['vendor_code'] == vendor_code]
            logger.info(f"Для артикула {vendor_code} найдено {len(vendor_code_orders)} заказов в кластере {cluster.name}")

            product_sold = {x: 0 for x in range(datetime.now().isocalendar()[1] - period_transactions,
                                                    datetime.now().isocalendar()[1]+1)}
            avg_price_by_week = {x: 0 for x in range(datetime.now().isocalendar()[1] - period_transactions,
                                                     datetime.now().isocalendar()[1] + 1)}

            for order in vendor_code_orders:
                product_sold[order['week']] += order['quantity']
                avg_price_by_week[order['week']] += order['price']

            total_sold = sum([product_sold[x] for x in product_sold.keys()])
            avg_count_per_week = total_sold / period_transactions
            rms_deviation = math.sqrt(1 / period_transactions * math.pow(
                sum([(product_sold[x] - avg_count_per_week) for x in product_sold.keys()]), 2))

            # Текущий остаток в кластере
            stock_search = [s for s in stocks if s.vendor_code == vendor_code and s.warehouse in cluster.warehouses]
            if len(stock_search) == 0:
                logger.warning(f"Нет стока для {vendor_code}, кластер: {cluster.name}")
                stock = 0
            else:
                stock = sum([s.free_to_sell_amount for s in stock_search])
            logger.info(f"Число остатков: {stock} для {vendor_code} в {cluster.name}")

            style = None
            predication_fos = PredictionFOS()
            predication_fof = PredictionFOF()
            if avg_count_per_week > 0:
                predication_fos = get_basic_predication_supplies_fos(current_stock=stock,
                                                                     avg_consumption=avg_count_per_week,
                                                                     deviation_sales=rms_deviation,
                                                                     size_supply=size_supply,
                                                                     supply_delivery_time=timedelta(days=6))
                predication_fof = get_basic_predication_supplies_fof(current_stock=stock,
                                                                     avg_consumption=avg_count_per_week,
                                                                     deviation_sales=rms_deviation,
                                                                     supply_delivery_time=timedelta(days=6),
                                                                     period=timedelta(days=period_supply))

                if (predication_fos.supply_date or predication_fof.supply_date) < timedelta(days=6):
                    style = "orange_red1 on white"
                if predication_fos.supply_date.days < 0:
                    predication_fos.supply_date = timedelta(days=0)
                if predication_fof.supply_date.days < 0:
                    predication_fof.supply_date = timedelta(days=0)

            if avg_count_per_week == 0 or stock == 0:
                style = "red on white"

            table.add_row(
                cluster.name,
                vendor_code,
                str(stock),
                str(total_sold),
                str(predication_fos.supply_date.days),
                (datetime.now() + predication_fos.supply_date).strftime("%d.%m.%Y"),
                "{:.2f}".format(predication_fof.supply_size),
                (datetime.now() + predication_fof.supply_date).strftime("%d.%m.%Y"),
                style=style
            )

    console.print(table)


if __name__ == '__main__':
    main()
