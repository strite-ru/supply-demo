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
from strite_data_hub.prediction.supplies.catboost import get_prediction
from strite_data_hub.parsers.ozon.utils import get_clusters_with_warehouses, OzonCluster
from tqdm import tqdm

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)
logging.captureWarnings(True)
logger = logging.getLogger(__name__)
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


def main(period_transactions: int = 30):
    api = init_data()

    postings: List[OzonFBOPosting] = list(OzonFBOPosting.get_postings(api,
                                                                      status="delivered",
                                                                      date_from=(datetime.now() - timedelta(
                                                                          weeks=period_transactions))))

    logger.info(f"Всего отправлений: {len(postings)}")
    # convert to product list from postings
    vendor_codes = []
    for product in postings:
        for order in product.orders:
            if order.vendor_code not in vendor_codes:
                vendor_codes.append(order.vendor_code)

    # selected_products = vendor_codes
    selected_products: List[str] = select_vendor_codes(vendor_codes)
    logger.info(f"Выбрано {len(selected_products)} товаров")

    # TODO выбор склада отгрузки
    cluster_from = get_cluster_by_name('Северо-запад')
    size_supply = int(input("Введите размер поставки: "))
    period_supply = int(input("Введите период поставки: "))
    prepare_days = int(input("Введите время подготовки (дни): "))

    orders = []
    for posting in postings:
        cluster_from = get_cluster_by_warehouse_id(str(posting.warehouse['id']))
        cluster_to = get_cluster_by_region(posting.warehouse['region'])
        if (cluster_from is None) or (cluster_to is None):
            logger.error(f"Не удалось определить кластер для склада {posting.warehouse}")
        else:
            for order in posting.orders:
                if order.vendor_code in selected_products:
                    # update route if vendor_code, from, to, week exists
                    if next((r for r in orders if
                             r['vendor_code'] == order.vendor_code and r['from'] == cluster_from and r[
                                 'to'] == cluster_to and r['week'] == posting.processTo.isocalendar()[1]), None):
                        route = next((r for r in orders if
                                      r['vendor_code'] == order.vendor_code and r['from'] == cluster_from and r[
                                          'to'] == cluster_to and r['week'] == posting.processTo.isocalendar()[1]),
                                     None)
                        route['quantity'] += order.quantity
                        route['price'] += order.price
                    else:
                        orders.append({
                            "from": cluster_from,
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
    table = Table(show_footer=False)
    table.title = "Артикулы магазина"
    table.add_column("Артикул", no_wrap=True)
    table.add_column("Кластер", no_wrap=True)
    table.add_column("Остаток", justify="center")
    table.add_column("Всего продано", justify="center")
    table.add_column("Продаж в неделю", justify="center")
    table.add_column("До поставки", justify="center")
    table.add_column("Дата поставки", justify="center")
    table.add_column("Размер поставки", justify="center")
    table.add_column("Дата поставки", justify="center")
    table.add_column("ML поставка", justify="center")

    for vendor_code in tqdm(selected_products):
        # find all orders with vendor_code
        vendor_orders = [o for o in orders if o['vendor_code'] == vendor_code and o['from'] == cluster_from]
        logger.info(f"Для артукула {vendor_code} найдено {len(vendor_orders)} заказов")
        if len(vendor_orders) == 0:
            continue
        # find all clusters to
        clusters_to = []
        for order in vendor_orders:
            if order['to'] not in clusters_to:
                clusters_to.append(order['to'])
        logger.info(f"Для артукула {vendor_code} найдено {len(clusters_to)} кластеров")

        for cluster_to in clusters_to:
            cluster_orders = [o for o in vendor_orders if o['to'] == cluster_to]
            logger.info(f"{len(cluster_orders)} заказов на кластер {cluster_to.name}")

            product_sold = {x: 0 for x in range(datetime.now().isocalendar()[1] - period_transactions,
                                                datetime.now().isocalendar()[1])}
            for order in cluster_orders:
                product_sold[order['week']] += order['quantity']

            total_sold = sum([product_sold[x] for x in product_sold.keys()])

            avg_price_by_week = {x: 0 for x in range(datetime.now().isocalendar()[1] - period_transactions,
                                                     datetime.now().isocalendar()[1])}
            for order in cluster_orders:
                avg_price_by_week[order['week']] += order['price']

            avg_count_per_week = total_sold / period_transactions
            rms_deviation = math.sqrt(1 / period_transactions * math.pow(
                sum([(product_sold[x] - avg_count_per_week) for x in product_sold.keys()]), 2))

            # TODO Срок кросс-докинга (из матрицы доставки)
            delivery_time = timedelta(days=6)

            # Среднее время доставки (из матрицы доставки) + обработки
            avg_delivery_time = delivery_time + timedelta(days=prepare_days)

            # Текущий остаток в кластере
            stock_search = [s for s in stocks if s.vendor_code == vendor_code and s.warehouse in cluster_to.warehouses]
            if len(stock_search) == 0:
                logger.warning(f"Нет стока для {vendor_code}, кластер: {cluster_to.name}")
                stock = 0
            else:
                stock = sum([s.free_to_sell_amount for s in stock_search])
            logger.info(f"Число остатков: {stock} для {vendor_code} в {cluster_to.name}")

            style = None
            predication_fos = PredictionFOS()
            predication_fof = PredictionFOF()
            if avg_count_per_week > 0:
                predication_fos = get_basic_predication_supplies_fos(current_stock=stock,
                                                                     avg_consumption=avg_count_per_week,
                                                                     deviation_sales=rms_deviation,
                                                                     size_supply=size_supply,
                                                                     supply_delivery_time=avg_delivery_time)
                predication_fof = get_basic_predication_supplies_fof(current_stock=stock,
                                                                     avg_consumption=avg_count_per_week,
                                                                     deviation_sales=rms_deviation,
                                                                     supply_delivery_time=avg_delivery_time,
                                                                     period=timedelta(days=period_supply))

                if (predication_fos.supply_date or predication_fof.supply_date) < avg_delivery_time:
                    style = "orange_red1 on white"
                if predication_fos.supply_date.days < 0:
                    predication_fos.supply_date = timedelta(days=0)
                if predication_fof.supply_date.days < 0:
                    predication_fof.supply_date = timedelta(days=0)

            if avg_count_per_week == 0 or stock == 0:
                style = "red on white"


            data_for_ml = []
            for week in range(datetime.now().isocalendar()[1] - period_transactions,
                              datetime.now().isocalendar()[1]):
                data_for_ml.append({
                    'week': week,
                    'vendor_code': vendor_code,
                    'log_sales_total': product_sold[week],
                    'avg_price': avg_price_by_week[week],
                    'cluster': cluster_to.name
                })
            predicts = [{
                'vendor_code': vendor_code,
                'week': datetime.now().isocalendar()[1],
                'avg_price': avg_price_by_week[datetime.now().isocalendar()[1]-1]
            }]
            results = get_prediction(data_for_ml, predicts)

            table.add_row(
                vendor_code,
                cluster_to.name,
                str(stock),
                str(total_sold),
                "{:.2f}".format(avg_count_per_week),
                str(predication_fos.supply_date.days),
                (datetime.now() + predication_fos.supply_date).strftime("%d.%m.%Y"),
                "{:.2f}".format(predication_fof.supply_size),
                (datetime.now() + predication_fof.supply_date).strftime("%d.%m.%Y"),
                "{:.2f}".format(results[0]),
                style=style
            )

    console.print(table)


if __name__ == '__main__':
    main()
