import time, json
from typing import Dict, List
from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository
import uuid
from transliterate import translit # added in requirements.txt


class DdsMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                producer: KafkaProducer,
                dds_repository: DdsRepository,
                logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
        
            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg["payload"]

            self._logger.info(f"{datetime.utcnow()}: ===================================================")
            self._logger.info(f"{datetime.utcnow()}: {order}")
            self._logger.info(f"{datetime.utcnow()}: ===================================================")

            self._dds_repository.h_user_insert(
                uuid.uuid1(),
                order["user"]["id"])
            user_uuid = self._dds_repository.user_hk_take(order["user"]["id"])
            self._dds_repository.s_user_names_insert(
                user_uuid,
                order["user"]["name"],
                order["user"]["login"],
                uuid.uuid1())
            
            self._dds_repository.h_restaurant_insert(
                uuid.uuid1(),
                order["restaurant"]["id"])
            restaurant_uuid = self._dds_repository.restaurant_hk_take(order["restaurant"]["id"])
            
            self._dds_repository.s_restaurant_names_insert(
                restaurant_uuid,
                order["restaurant"]["name"],
                uuid.uuid1(),)

            self._dds_repository.h_order_insert(
                uuid.uuid1(),
                order["id"],
                order["date"])
            order_uuid = self._dds_repository.order_hk_take(order["id"])

            self._dds_repository.s_order_cost_insert(
                order_uuid,
                order["cost"],
                order["payment"],
                uuid.uuid1())

            self._dds_repository.s_order_status_insert(
                order_uuid,
                order["status"],
                uuid.uuid1())
            
            self._dds_repository.l_order_user_insert(
                uuid.uuid1(),
                order_uuid,
                user_uuid)

            products = []

            for i in order["products"]:

                self._dds_repository.h_product_insert(
                    uuid.uuid1(),
                    i["id"])
                product_uuid = self._dds_repository.product_hk_take(i["id"])
                
                self._dds_repository.s_product_names_insert(
                    product_uuid,
                    i["name"],
                    uuid.uuid1()),
                

                self._dds_repository.h_category_insert(
                    uuid.uuid1(),
                    i["category"])
                category_uuid = self._dds_repository.category_hk_take(i["category"])

                self._dds_repository.l_product_category_insert(
                    uuid.uuid1(),
                    category_uuid,
                    product_uuid)

                self._dds_repository.l_product_restaurant_insert(
                    uuid.uuid1(),
                    restaurant_uuid,
                    product_uuid)

                self._dds_repository.l_order_product_insert(
                    uuid.uuid1(),
                    order_uuid,
                    product_uuid)
                
                
                products.append(self._format_products(category_uuid, i["category"], product_uuid, i["name"], i['quantity']))


                self._logger.info(f"{datetime.utcnow()}: ====================")
                self._logger.info(f"{datetime.utcnow()}: {category_uuid}")
                self._logger.info(f"{datetime.utcnow()}: {product_uuid}")
                self._logger.info(f"{datetime.utcnow()}: ====================")

            dds_dst_msg = {
                "object_id": order["id"],
                "object_type": msg["object_type"],
                "payload": {"order_id": order_uuid,
                            "user_id": user_uuid,
                            "products": products
                }
            }

            self._logger.info(f"{datetime.utcnow()}: ====================")
            self._logger.info(f"{datetime.utcnow()}: {dds_dst_msg}")
            self._logger.info(f"{datetime.utcnow()}: ====================")

            self._producer.produce(dds_dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _format_products(self, category_id, category_name, product_id, product_name, quantity) -> Dict[str, str]:
        return {
			"category_id": category_id,
			"category_name": category_name,
			"product_id": product_id,
			"product_name": product_name,
            "quantity": quantity
		}