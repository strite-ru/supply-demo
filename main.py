import json
import logging
import math
from typing import List
import pick
from datetime import timedelta, datetime

from rich.console import Console
from rich.table import Table
from strite_data_hub.parsers.ozon import OzonAPI, OzonWarehouse, OzonProduct, OzonStockOnWarehouse, OzonTransaction, \
    OzonPosting
from strite_data_hub.prediction.supplies.basic import get_basic_predication_supplies_fos, \
    get_basic_predication_supplies_fof
from tqdm import tqdm


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)
logging.captureWarnings(True)
logger = logging.getLogger(__name__)


def init_data() -> OzonAPI:
    client_id = input("Client-Id: ")
    api_key = input("Api-Key: ")

    return OzonAPI(client_id=int(client_id), key=api_key)


def select_products(products: List[OzonProduct]) -> List[OzonProduct]:
    products_vendor_codes = [product.vendor_code for product in products]
    products_vendor_codes.sort()
    selected_products = pick.pick(products_vendor_codes, "Выберите товары, которые будут отправляться:\nЕсли ничего не выбрано то все товары",
                                               multiselect=True)

    if not selected_products:
        return products

    selected_products_vendor_codes = [vendor_code for vendor_code, _ in selected_products]
    return [product for product in products if product.vendor_code in selected_products_vendor_codes]


def select_warehouses_from(w: List[dict], warehouses: List[OzonWarehouse]) -> OzonWarehouse:
    warehouses_local_from = [warehouse.get("name", "None") for warehouse in w if
                             len(warehouse.get("cross-docking", []))]
    from_name = pick.pick(warehouses_local_from, "Выберите склад, с которого будут забираться товары:")[0]
    warehouse_from = [w for w in w if w.get("name", "None") == from_name][0]
    return [w for w in warehouses if w.id == warehouse_from.get("id", None)][0]


def select_warehouses_to(w: List[dict], warehouses: List[OzonWarehouse]) -> OzonWarehouse:
    warehouses_local_to = [warehouse.get("name", "None") for warehouse in w]
    to_name = pick.pick(warehouses_local_to, "Выберите склад, на который будут отправляться товары:")[0]
    warehouse_to = [w for w in w if w.get("name", "None") == to_name][0]
    return [w for w in warehouses if w.id == warehouse_to.get("id", None)][0]


def main(period_transactions: int = 29):
    warehouses_local = json.load(open('./warehouses.json', encoding="utf8"))

    api = init_data()

    try:
        products: List[OzonProduct] = list(OzonProduct.get_products(api))
        selected_products: List[OzonProduct] = select_products(products)
        logger.info(f"Выбрано {len(selected_products)} товаров")
    except Exception as e:
        logger.error(e)
        exit(1)

    warehouses = list(OzonWarehouse.get_warehouses(api))
    warehouse_from = select_warehouses_from(warehouses_local, warehouses)
    ws_id = [ww.get("id") for ww in [w for w in warehouses_local if w.get("id", None) == warehouse_from.id][0].get("cross-docking", [])]
    warehouse_to = select_warehouses_to([w for w in warehouses_local if w.get("id", None) in ws_id], warehouses)

    logger.info(f"Склад приема: {warehouse_to.name}")
    logger.info(f"Склад отправки: {warehouse_from.name}")

    wl = [w for w in warehouses_local if w.get("id", None) == warehouse_from.id][0]
    dt = [w for w in wl.get("cross-docking", []) if w.get("id", None) == warehouse_to.id][0].get("duration_max", 0)
    delivery_time = timedelta(days=dt)
    del wl, dt
    logger.info(f"Время доставки: {delivery_time}")

    size_supply = int(input("Введите размер поставки: "))
    period_supply = int(input("Введите период поставки: "))
    prepare_days = int(input("Введите время подготовки (дни): "))

    stocks = list(OzonStockOnWarehouse.get_stocks(api))

    # Получаем данные о транзакциях
    transactions = list(OzonTransaction.get_transactions(
        api=api,
        start_date=datetime.now() - timedelta(days=period_transactions),
        end_date=datetime.now()
    ))

    logger.info(f"Всего транзакций за период: {len(transactions)}")
    # Фильтруем операции по FBO
    fbo_transactions = [tr for tr in transactions if tr.order_type == 0]
    logger.info(f"Все операций по FBO: {len(fbo_transactions)}")

    # Список артикулов за период
    skus = list({tr.marketplace_product for tr in fbo_transactions})
    logger.info(f"Всего sku за период: {len(skus)}")

    # Конфигурация вывода данных
    console = Console(safe_box=False, force_terminal=True)
    console.size = (250, 38)
    table = Table(show_footer=False)
    table.title = "Артикулы магазина"
    table.add_column("Артикул", no_wrap=True)
    table.add_column("Склад", no_wrap=True)
    table.add_column("Остаток", justify="center")
    table.add_column("Всего продано", justify="center")
    table.add_column("Среднее число продаж в день", justify="center")
    table.add_column("Среднеквадратичное отклонение", justify="center")
    table.add_column("До поставки", justify="center")
    table.add_column("Дата поставки", justify="center")
    table.add_column("Размер поставки", justify="center")
    table.add_column("Дата поставки", justify="center")

    for sku in tqdm(skus):
        def get_product() -> OzonProduct | None:
            _p = None
            for item in selected_products:
                if next((item for _size in item.sizes if _size.id == sku and _size.type == 'fbo'), None):
                    _p = item
            return _p
        logger.info(f"Обрабатываем sku {sku}")
        product: OzonProduct | None = get_product()

        if product is None:
            logger.info(f"Мы пропускаем sku {sku}. Нет информации о товаре")
            continue

        # Транзакции по товару
        product_transactions = [tr for tr in fbo_transactions if (tr.marketplace_product == sku)]
        logger.info(f"Всего транзакций по товару: {len(product_transactions)}")
        # Отправления по товару
        product_postings = list({tr.order_id for tr in product_transactions})
        product_sold = {x: 0 for x in range(period_transactions)}

        for p_n in product_postings:
            posting = OzonPosting.get_fbo_posting_by_posting_number(api, p_n)
            count = sum([o.quantity for o in posting.orders if o.vendor_code == product.vendor_code], 0)
            if str(posting.warehouse_id) == warehouse_to.id:
                product_sold[(datetime.now() - posting.processTo).days] = count

        total_sold = sum([product_sold[x] for x in product_sold.keys()])
        logger.info(f"Всего продано: {total_sold}")

        avg_count_per_day = total_sold / period_transactions
        logger.info(f"Среднее число продаж в день: {avg_count_per_day}")
        rms_deviation = math.sqrt(1/period_transactions * math.pow(sum([(product_sold[x] - avg_count_per_day) for x in product_sold.keys()]), 2))
        if avg_count_per_day == 0:
            avg_count_per_day = 0.01

        # Среднее время доставки (из матрицы доставки) + обработки
        avg_delivery_time = delivery_time + timedelta(days=prepare_days)

        # Текущий остаток на складе
        stock_search = [s for s in stocks if s.vendor_code == product.vendor_code and s.warehouse == warehouse_to]
        if len(stock_search) == 0:
            logger.warning(f"Нет стока для {product.vendor_code}, склад: {warehouse_to.name}")
            stock = 0
        else:
            stock = stock_search[0].free_to_sell_amount

        predication_fos = get_basic_predication_supplies_fos(current_stock=stock,
                                                             avg_consumption=avg_count_per_day,
                                                             deviation_sales=rms_deviation,
                                                             size_supply=size_supply,
                                                             supply_delivery_time=avg_delivery_time)
        predication_fof = get_basic_predication_supplies_fof(current_stock=stock,
                                                             avg_consumption=avg_count_per_day,
                                                             deviation_sales=rms_deviation,
                                                             supply_delivery_time=avg_delivery_time,
                                                             period=timedelta(days=period_supply))
        style = None
        if predication_fos.supply_date.days < period_supply:
            style = "red on white"

        table.add_row(
            product.vendor_code,
            warehouse_to.name,
            str(stock),
            str(total_sold),
            "{:.2f}".format(avg_count_per_day),
            "{:.2f}".format(rms_deviation),
            str(predication_fos.supply_date.days),
            (datetime.now() + predication_fos.supply_date).strftime("%d.%m.%Y"),
            "{:.2f}".format(predication_fof.supply_size),
            (datetime.now() + predication_fof.supply_date).strftime("%d.%m.%Y"),
            style=style
        )

    console.print(table)


if __name__ == '__main__':
    main()
