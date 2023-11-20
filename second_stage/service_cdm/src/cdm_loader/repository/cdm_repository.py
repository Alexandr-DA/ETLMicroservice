import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
    
    def category_hk_take(self,
                        category_name: str
                        ) -> str:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT h_category_pk 
                        FROM dds.h_category
                        WHERE category_name = %(category_name)s;
                    """,
                    {
                        'category_name': category_name,                
                    }
                )
                pk = str(cur.fetchone()[0])
                return(pk)

    def product_hk_take(self,
                        product_id: str
                        ) -> str:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT h_product_pk
                        FROM dds.h_product
                        WHERE product_id = %(product_id)s;
                    """,
                    {
                        'product_id': product_id,                
                    }
                )
                pk = str(cur.fetchone()[0])
                return(pk)

    def user_hk_take(self,
                        user_id: str
                        ) -> str:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT h_user_pk
                        FROM dds.h_user
                        WHERE user_id = %(user_id)s;
                    """,
                    {
                        'user_id': user_id,                
                    }
                )
                pk = str(cur.fetchone()[0])
                return(pk)
            
    def user_product_counters_insert(self,
                        user_id: str,
                        product_id: str,
                        product_name: str,
                        order_cnt: int
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
						VALUES(%(user_id)s, %(product_id)s, %(product_name)s, %(order_cnt)s)
						ON CONFLICT (user_id, product_id) DO UPDATE
						SET order_cnt = user_product_counters.order_cnt + EXCLUDED.order_cnt,
						product_name = EXCLUDED.product_name;
                    """,
                    {
                        'user_id': user_id,
                        'product_id': product_id,
                        'product_name': product_name,
                        'order_cnt': order_cnt
                    }
                )

    def user_category_counters_insert(self,
                        user_id: str,
                        category_id: str,
                        category_name: str,
                        order_cnt: int
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
						VALUES(%(user_id)s, %(category_id)s, %(category_name)s, %(order_cnt)s)
						ON CONFLICT (user_id, category_id) DO UPDATE
						SET order_cnt = user_category_counters.order_cnt + EXCLUDED.order_cnt,
						category_name = EXCLUDED.category_name;
                    """,
                    {
                        'user_id': user_id,
                        'category_id': category_id,
                        'category_name': category_name,
                        'order_cnt': order_cnt
                    }
                )

# Запасные варианты со взятием hk_id из dds слоя с помощью базового order_id (рабочие, проверено)

    def user_product_counters_insert_2(self,
                        _id: int,
                        product_name: str,
                        ordcnt: int
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """ 
                        INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                        SELECT h_user.h_user_pk, h_product.h_product_pk, %(product_name)s::varchar, %(ord_cnt)s::int
                        FROM dds.h_order 
                        JOIN dds.l_order_user ON l_order_user.h_order_pk = h_order.h_order_pk
                        JOIN dds.h_user ON h_user.h_user_pk = l_order_user.h_user_pk 
                        JOIN dds.l_order_product ON h_order.h_order_pk = l_order_product.h_order_pk
                        JOIN dds.h_product ON l_order_product.h_product_pk = h_product.h_product_pk
                        JOIN dds.s_product_names ON s_product_names.h_product_pk = h_product.h_product_pk
                        WHERE s_product_names.name = %(product_name)s AND h_order.order_id = %(_id)s
						ON CONFLICT (user_id, product_id) DO UPDATE
						SET order_cnt = user_product_counters.order_cnt + EXCLUDED.order_cnt,
						product_name = EXCLUDED.product_name;
                    """,
                    {
                        '_id': _id,
                        'product_name': product_name,
                        'ord_cnt': ordcnt
                    }
                )

    def user_category_counters_insert_2(self,
                        _id: int,
                        category_name: str,
                        order_cnt: int
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """ 
                        INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                        SELECT h_user.h_user_pk, h_category.h_category_pk, %(category_name)s::varchar, %(order_cnt)s::int
                        FROM dds.h_order 
                        JOIN dds.l_order_user ON l_order_user.h_order_pk = h_order.h_order_pk
                        JOIN dds.h_user ON h_user.h_user_pk = l_order_user.h_user_pk 
                        JOIN dds.l_order_product ON h_order.h_order_pk = l_order_product.h_order_pk
                        JOIN dds.h_product ON l_order_product.h_product_pk = h_product.h_product_pk
                        JOIN dds.l_product_category ON h_product.h_product_pk = l_product_category.h_product_pk
                        JOIN dds.h_category ON l_product_category.h_category_pk = h_category.h_category_pk
                        WHERE h_category.category_name = %(category_name)s AND h_order.order_id = %(_id)s
						ON CONFLICT (user_id, category_id) DO UPDATE
						SET order_cnt = user_category_counters.order_cnt + EXCLUDED.order_cnt,
						category_name = EXCLUDED.category_name;
                    """,
                    {
                        '_id': _id,
                        'category_name': category_name,
                        'order_cnt': order_cnt
                    }
                )