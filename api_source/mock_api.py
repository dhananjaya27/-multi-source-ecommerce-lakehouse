from fastapi import FastAPI

app = FastAPI(title="E-Commerce Mock API")


@app.get("/")
def home():
    return {"message": "E-Commerce Mock API is running"}


@app.get("/shipments")
def get_shipments():
    return [
        {
            "shipment_id": "SHP001",
            "order_id": 1,
            "carrier": "BlueDart",
            "tracking_id": "TRK1001",
            "shipment_status": "DELIVERED",
            "shipped_date": "2026-05-10",
            "delivered_date": "2026-05-12",
        },
        {
            "shipment_id": "SHP002",
            "order_id": 2,
            "carrier": "Delhivery",
            "tracking_id": "TRK1002",
            "shipment_status": "IN_TRANSIT",
            "shipped_date": "2026-05-11",
            "delivered_date": None,
        },
    ]


@app.get("/product-reviews")
def get_product_reviews():
    return [
        {
            "review_id": "REV001",
            "product_id": 101,
            "customer_id": 1,
            "rating": 4,
            "review_text": "Good product and value for money.",
            "review_date": "2026-05-10",
        },
        {
            "review_id": "REV002",
            "product_id": 102,
            "customer_id": 2,
            "rating": 5,
            "review_text": "Excellent quality and fast delivery.",
            "review_date": "2026-05-11",
        },
    ]


@app.get("/support-tickets")
def get_support_tickets():
    return [
        {
            "ticket_id": "TCK001",
            "customer_id": 1,
            "order_id": 1,
            "issue_type": "Delivery Delay",
            "priority": "Medium",
            "status": "OPEN",
            "created_date": "2026-05-10",
        },
        {
            "ticket_id": "TCK002",
            "customer_id": 2,
            "order_id": 2,
            "issue_type": "Payment Failed",
            "priority": "High",
            "status": "RESOLVED",
            "created_date": "2026-05-11",
        },
    ]