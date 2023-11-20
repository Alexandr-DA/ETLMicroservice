
from datetime import datetime
from typing import Any, Dict, List

from lib.pg.pg_connect import PgConnect
from pydantic import BaseModel



class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
    
    def h_user_insert(self,
                        h_pk: str,
                        _id: str
                        ) -> None:

##################### HUBS

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user (h_user_pk, user_id, load_dt)
						VALUES(%(h_pk)s, %(_id)s, %(load_dt)s)
						ON CONFLICT (user_id) DO UPDATE
						SET user_id = EXCLUDED.user_id,
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'h_pk': h_pk,
                        '_id': _id,
                        'load_dt': datetime.utcnow()
                    }
                )

    def h_product_insert(self,
                        h_pk: str,
                        _id: str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product (h_product_pk, product_id, load_dt)
						VALUES(%(h_pk)s, %(_id)s, %(load_dt)s)
						ON CONFLICT (product_id) DO UPDATE
						SET product_id = EXCLUDED.product_id,
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'h_pk': h_pk,
                        '_id': _id,
                        'load_dt': datetime.utcnow()
                    }
                )

    def h_category_insert(self,
                        h_pk: str,
                        _id: str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category (h_category_pk, category_name, load_dt)
						VALUES(%(h_pk)s, %(_id)s, %(load_dt)s)
						ON CONFLICT (category_name) DO UPDATE
						SET category_name = EXCLUDED.category_name,
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'h_pk': h_pk,
                        '_id': _id,
                        'load_dt': datetime.utcnow()
                    }
                )

    def h_restaurant_insert(self,
                        h_pk: str,
                        _id: str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt)
						VALUES(%(h_pk)s, %(_id)s, %(load_dt)s)
						ON CONFLICT (restaurant_id) DO UPDATE
						SET restaurant_id = EXCLUDED.restaurant_id,
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'h_pk': h_pk,
                        '_id': _id,
                        'load_dt': datetime.utcnow()
                    }
                )

    def h_order_insert(self,
                        h_pk: str,
                        _id: str,
                        order_dt: datetime
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt)
						VALUES(%(h_pk)s, %(_id)s, %(order_dt)s, %(load_dt)s)
						ON CONFLICT (order_id) DO UPDATE
						SET order_id = EXCLUDED.order_id,
                        order_dt = EXCLUDED.order_dt,
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'h_pk': h_pk,
                        '_id': _id,
                        'order_dt': order_dt,
                        'load_dt': datetime.utcnow()
                    }
                )

##################### LINKS


    def l_order_product_insert(self,
                        hk_pk: str,
                        h_order_pk: str,
                        h_product_pk: str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt)
						VALUES(%(hk_pk)s, %(h_order_pk)s, %(h_product_pk)s, %(load_dt)s)
						ON CONFLICT (h_order_pk, h_product_pk) DO UPDATE
						SET h_order_pk = EXCLUDED.h_order_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'hk_pk': hk_pk,
                        'h_order_pk': h_order_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': datetime.utcnow()
                    }
                )

    def l_product_restaurant_insert(self,
                        hk_pk: str,
                        h_restaurant_pk: str,
                        h_product_pk: str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt)
						VALUES(%(hk_pk)s, %(h_restaurant_pk)s, %(h_product_pk)s, %(load_dt)s)
						ON CONFLICT (h_restaurant_pk, h_product_pk) DO UPDATE
						SET h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'hk_pk': hk_pk,
                        'h_restaurant_pk': h_restaurant_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': datetime.utcnow()
                    }
                )

    def l_product_category_insert(self,
                        hk_pk: str,
                        h_category_pk: str,
                        h_product_pk: str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt)
						VALUES(%(hk_pk)s, %(h_category_pk)s, %(h_product_pk)s, %(load_dt)s)
						ON CONFLICT (h_category_pk, h_product_pk) DO UPDATE
						SET h_category_pk = EXCLUDED.h_category_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'hk_pk': hk_pk,
                        'h_category_pk': h_category_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': datetime.utcnow()
                    }
                )

    def l_order_user_insert(self,
                        hk_pk: str,
                        h_order_pk: str,
                        h_user_pk: str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt)
						VALUES(%(hk_pk)s, %(h_order_pk)s, %(h_user_pk)s, %(load_dt)s)
						ON CONFLICT (h_order_pk, h_user_pk) DO UPDATE
						SET h_order_pk = EXCLUDED.h_order_pk,
                        h_user_pk = EXCLUDED.h_user_pk,
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'hk_pk': hk_pk,
                        'h_order_pk': h_order_pk,
                        'h_user_pk': h_user_pk,
                        'load_dt': datetime.utcnow()
                    }
                )

##################### SATELITS
    
    def s_user_names_insert(self,
                        id_pk: str,
                        username: str,
                        userlogin: str,
                        hk_pk: str
                        ) -> None:
            
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, hk_user_names_hashdiff)
						VALUES(%(id_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(hk_pk)s)
						ON CONFLICT (h_user_pk) DO UPDATE
						SET h_user_pk = EXCLUDED.h_user_pk,
						username = EXCLUDED.username,
						userlogin = EXCLUDED.userlogin,                        
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        
                        'id_pk': id_pk,
                        'username': username,
                        'userlogin': userlogin,
                        'load_dt': datetime.utcnow(),
                        'hk_pk': hk_pk
                    }
                )

    def s_product_names_insert(self,
                        id_pk: str,
                        name: str,
                        hk_pk: str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names (h_product_pk, name, load_dt, hk_product_names_hashdiff)
						VALUES(%(id_pk)s, %(name)s, %(load_dt)s, %(hk_pk)s)
						ON CONFLICT (h_product_pk) DO UPDATE
						SET h_product_pk = s_product_names.h_product_pk,
						name = EXCLUDED.name,                      
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'hk_pk': hk_pk,
                        'id_pk': id_pk,
                        'name': name,
                        'load_dt': datetime.utcnow()
                    }
                )

    def s_restaurant_names_insert(self,
                        id_pk: str,
                        name: str,
                        hk_pk: str,
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_dt, hk_restaurant_names_hashdiff)
						VALUES(%(id_pk)s, %(name)s, %(load_dt)s, %(hk_pk)s)
						ON CONFLICT (h_restaurant_pk) DO UPDATE
						SET h_restaurant_pk = s_restaurant_names.h_restaurant_pk,
						name = EXCLUDED.name,                      
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'id_pk': id_pk,
                        'name': name,
                        'load_dt': datetime.utcnow(),
                        'hk_pk': hk_pk
                    }
                )

    def s_order_cost_insert(self,
                        id_pk: str,
                        cost: int,
                        payment: int,
                        hk_pk: str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, hk_order_cost_hashdiff )
						VALUES(%(id_pk)s, %(cost)s, %(payment)s, %(load_dt)s, %(hk_pk)s)
						ON CONFLICT (h_order_pk) DO UPDATE
						SET h_order_pk = EXCLUDED.h_order_pk,
						cost = EXCLUDED.cost,
						payment = EXCLUDED.payment,                        
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'id_pk': id_pk,
                        'cost': cost,
                        'payment': payment,
                        'load_dt': datetime.utcnow(),
                        'hk_pk': hk_pk
                    }
                )

    def s_order_status_insert(self,
                        id_pk: str,
                        status: str,
                        hk_pk: str
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, hk_order_status_hashdiff)
						VALUES(%(id_pk)s, %(status)s, %(load_dt)s, %(hk_pk)s)
						ON CONFLICT (h_order_pk) DO UPDATE
						SET h_order_pk = EXCLUDED.h_order_pk,
						status = EXCLUDED.status,                    
						load_dt = EXCLUDED.load_dt;
                    """,
                    {
                        'id_pk': id_pk,
                        'status': status,
                        'load_dt': datetime.utcnow(),
                        'hk_pk': hk_pk
                    }
                )

##################### TAKES

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


    def restaurant_hk_take(self,
                        restaurant_id: str
                        ) -> str:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT h_restaurant_pk
                        FROM dds.h_restaurant
                        WHERE restaurant_id = %(restaurant_id)s;
                    """,
                    {
                        'restaurant_id': restaurant_id,                
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

    def order_hk_take(self,
                        order_id: str
                        ) -> str:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT h_order_pk
                        FROM dds.h_order
                        WHERE order_id = %(order_id)s;
                    """,
                    {
                        'order_id': order_id,                
                    }
                )
                pk = str(cur.fetchone()[0])
                return(pk)