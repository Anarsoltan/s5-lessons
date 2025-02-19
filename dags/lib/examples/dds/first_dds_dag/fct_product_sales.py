from lib import PgConnect
from psycopg import Connection
from typing import List, Dict
import json

class ProductSalesRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sales(self) -> List[Dict]:
        with self._db.client().cursor() as cur:
            cur.execute("""
                SELECT object_value FROM stg.ordersystem_orders;
            """)
            orders = cur.fetchall()
            sales_list = []

            for row in orders:
                try:
                    order_data = json.loads(row[0])
                    order_id = order_data.get("_id")
                    restaurant_ext_id = order_data["restaurant"]["id"]
                    user_id = order_data["user"]["id"]
                    
                    cur.execute("""
                        SELECT id FROM dds.dm_orders WHERE order_key = %s;
                    """, (order_id,))
                    order_result = cur.fetchone()
                    
                    cur.execute("""
                        SELECT id FROM dds.dm_restaurants WHERE restaurant_id = %s;
                    """, (restaurant_ext_id,))
                    restaurant_result = cur.fetchone()
                    
                    if not order_result or not restaurant_result:
                        continue  # Əgər uyğun id tapılmadısa, keçirik
                    
                    for item in order_data.get("order_items", []):
                        cur.execute("""
                            SELECT id FROM dds.dm_products WHERE product_id = %s;
                        """, (item["id"],))
                        product_result = cur.fetchone()
                        
                        if not product_result:
                            continue
                        
                        total_sum = item["price"] * item["quantity"]
                        bonus_payment = order_data.get("bonus_payment", 0) / len(order_data["order_items"])
                        bonus_grant = order_data.get("bonus_grant", 0) / len(order_data["order_items"])
                        
                        sales_data = {
                            "product_id": product_result[0],
                            "order_id": order_result[0],
                            "count": item["quantity"],
                            "price": item["price"],
                            "total_sum": total_sum,
                            "bonus_payment": bonus_payment,
                            "bonus_grant": bonus_grant,
                        }
                        sales_list.append(sales_data)
                except Exception as e:
                    print(f"Xəta sifarişin işlənməsində: {e}, JSON: {row[0]}")
                    continue
            
            return sales_list

    def insert_sales(self, sales: List[Dict]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for sale in sales:
                    try:
                        cur.execute("""
                            INSERT INTO dds.fct_product_sales (
                                product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            sale["product_id"],
                            sale["order_id"],
                            sale["count"],
                            sale["price"],
                            sale["total_sum"],
                            sale["bonus_payment"],
                            sale["bonus_grant"]
                        ))
                    except Exception as e:
                        print(f"Xəta fakt satışın daxil edilməsində: {e}, data: {sale}")
                        continue
            conn.commit()

class ProductSalesLoader:
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect) -> None:
        self.pg_dest = pg_dest
        self.stg = ProductSalesRepository(pg_dest)
        self.origin = ProductSalesRepository(pg_origin)

    def load_sales(self) -> None:
        sales_list = self.origin.list_sales()
        self.stg.insert_sales(sales_list)
