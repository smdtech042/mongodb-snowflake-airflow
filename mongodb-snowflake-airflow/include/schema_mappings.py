# schema_mappings.py

# Default MongoDB Database name provided by user
MONGO_DATABASE = "supplier_db"

# Central registry of all collections, their mapped table names, and field-level casting rules.
# Each field maps to the appropriate Snowflake SQL type.
# Nested objects and arrays are mapped to VARIANT.
SCHEMA_MAPPINGS = {
    "attributes": {
        "table_name": "RAW_ATTRIBUTES",
        "view_name": "ATTRIBUTES",
        "fields": {
            "category": "VARCHAR",
            "subcategory": "VARCHAR",
            "subsubcategory": "VARCHAR",
            "name": "VARCHAR",
            "required": "BOOLEAN",
            "type": "VARCHAR",
            "options": "VARIANT",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "categories": {
        "table_name": "RAW_CATEGORIES",
        "view_name": "CATEGORIES",
        "fields": {
            "name": "VARCHAR",
            "fields": "VARIANT",
            "customFields": "VARIANT"
        }
    },
    "chatsessions": {
        "table_name": "RAW_CHAT_SESSIONS",
        "view_name": "CHAT_SESSIONS",
        "fields": {
            "ticketId": "VARCHAR",
            "userId": "VARCHAR",
            "agentId": "VARCHAR",
            "messages": "VARIANT",
            "status": "VARCHAR",
            "startedAt": "TIMESTAMP_TZ",
            "endedAt": "TIMESTAMP_TZ"
        }
    },
    "counters": {
        "table_name": "RAW_COUNTERS",
        "view_name": "COUNTERS",
        "fields": {
            "name": "VARCHAR",
            "count": "NUMBER"
        }
    },
    "coupons": {
        "table_name": "RAW_COUPONS",
        "view_name": "COUPONS",
        "fields": {
            "code": "VARCHAR",
            "type": "VARCHAR",
            "value": "VARIANT",
            "usageLimit": "NUMBER",
            "usedCount": "NUMBER",
            "expiryDate": "TIMESTAMP_TZ",
            "isActive": "BOOLEAN",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "currencyrates": {
        "table_name": "RAW_CURRENCY_RATES",
        "view_name": "CURRENCY_RATES",
        "fields": {
            "rates": "VARIANT",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "documents": {
        "table_name": "RAW_DOCUMENTS",
        "view_name": "DOCUMENTS",
        "fields": {
            "productId": "VARCHAR",
            "documentType": "VARCHAR",
            "cloudinaryUrl": "VARCHAR",
            "uploadedBy": "VARCHAR",
            "uploadDate": "TIMESTAMP_TZ",
            "status": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "faqs": {
        "table_name": "RAW_FAQS",
        "view_name": "FAQS",
        "fields": {
            "question": "VARCHAR",
            "answer": "VARCHAR",
            "keywords": "VARIANT",
            "category": "VARCHAR",
            "viewCount": "NUMBER",
            "ratings": "VARIANT",
            "relatedTickets": "VARIANT",
            "createdAt": "TIMESTAMP_TZ"
        }
    },
    "usersinquiries": {
        "table_name": "RAW_USERS_INQUIRIES",
        "view_name": "USERS_INQUIRIES",
        "fields": {
            "supplier": "VARCHAR",
            "product": "VARCHAR",
            "unit": "VARCHAR",
            "quantity": "NUMBER",
            "content": "VARCHAR",
            "email": "VARCHAR",
            "productImg": "VARCHAR",
            "files": "VARIANT",
            "createdAt": "TIMESTAMP_TZ"
        }
    },
    "inquiries": {
        "table_name": "RAW_INQUIRIES",
        "view_name": "INQUIRIES",
        "fields": {
            "inquiryId": "VARCHAR",
            "name": "VARCHAR",
            "email": "VARCHAR",
            "contact": "VARCHAR",
            "topic": "VARCHAR",
            "message": "VARCHAR",
            "status": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "inquiryresponses": {
        "table_name": "RAW_INQUIRY_RESPONSES",
        "view_name": "INQUIRY_RESPONSES",
        "fields": {
            "inquiry": "VARCHAR",
            "supplier": "VARCHAR",
            "message": "VARCHAR",
            "price": "NUMBER",
            "currency": "VARCHAR",
            "deliveryTime": "VARCHAR",
            "additionalNotes": "VARCHAR",
            "files": "VARIANT",
            "status": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "notifications": {
        "table_name": "RAW_NOTIFICATIONS",
        "view_name": "NOTIFICATIONS",
        "fields": {
            "recipient": "VARCHAR",
            "recipientModel": "VARCHAR",
            "eventType": "VARCHAR",
            "message": "VARCHAR",
            "relatedEntity": "VARIANT",
            "details": "VARIANT",
            "status": "VARCHAR",
            "deliveredAt": "TIMESTAMP_TZ",
            "readAt": "TIMESTAMP_TZ",
            "failureReason": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "orders": {
        "table_name": "RAW_ORDERS",
        "view_name": "ORDERS",
        "fields": {
            "userId": "VARCHAR",
            "products": "VARIANT",
            "total": "NUMBER",
            "status": "VARCHAR",
            "address": "VARIANT",
            "orderId": "VARCHAR",
            "paymentId": "VARCHAR",
            "discountCode": "VARCHAR",
            "trackingUpdates": "VARIANT",
            "isGuest": "BOOLEAN",
            "guestEmail": "VARCHAR",
            "deliveryPartner": "VARCHAR",
            "supplierApprovalStatus": "VARCHAR",
            "supplier": "VARCHAR",
            "shipmentId": "VARCHAR",
            "trackingNumber": "VARCHAR",
            "paymentStatus": "VARCHAR",
            "invoiceUrl": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "paymentattempts": {
        "table_name": "RAW_PAYMENT_ATTEMPTS",
        "view_name": "PAYMENT_ATTEMPTS",
        "fields": {
            "userId": "VARCHAR",
            "status": "VARCHAR",
            "amount": "NUMBER",
            "currency": "VARCHAR",
            "orderId": "VARCHAR",
            "failureReason": "VARCHAR",
            "errorCode": "VARCHAR",
            "metadata": "VARIANT",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "products": {
        "table_name": "RAW_PRODUCTS",
        "view_name": "PRODUCTS",
        "fields": {
            "id": "VARCHAR",
            "category": "VARCHAR",
            "subcategory": "VARCHAR",
            "subsubcategory": "VARCHAR",
            "productName": "VARCHAR",
            "modelNumber": "VARCHAR",
            "images": "VARIANT",
            "thumbnailImage": "VARCHAR",
            "price": "VARCHAR",
            "stock": "NUMBER",
            "description": "VARCHAR",
            "searchKeywords": "VARIANT",
            "attributes": "VARIANT",
            "unitOfMeasure": "VARCHAR",
            "paymentMethod": "VARCHAR",
            "orderLeadTimeRange": "VARIANT",
            "fobPrices": "VARIANT",
            "additionalInfo": "VARIANT",
            "supplier": "VARCHAR",
            "listingStatus": "VARCHAR",
            "productStatus": "VARCHAR",
            "isDeleted": "BOOLEAN",
            "deletedAt": "TIMESTAMP_TZ",
            "isArchived": "BOOLEAN",
            "archivedAt": "TIMESTAMP_TZ",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "returns": {
        "table_name": "RAW_RETURNS",
        "view_name": "RETURNS",
        "fields": {
            "orderId": "VARCHAR",
            "userId": "VARCHAR",
            "supplierId": "VARCHAR",
            "reason": "VARCHAR",
            "subReason": "VARCHAR",
            "images": "VARIANT",
            "status": "VARCHAR",
            "returnShipmentId": "VARCHAR",
            "returnTrackingNumber": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "rfqs": {
        "table_name": "RAW_RFQS",
        "view_name": "RFQS",
        "fields": {
            "rfqId": "VARCHAR",
            "user": "VARCHAR",
            "productName": "VARCHAR",
            "productCategory": "VARCHAR",
            "productDetails": "VARCHAR",
            "files": "VARIANT",
            "orderQuantity": "NUMBER",
            "unit": "VARCHAR",
            "email": "VARCHAR",
            "autoSendAfter48h": "BOOLEAN",
            "suppliersNotified": "VARIANT",
            "status": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ"
        }
    },
    "rfqresponses": {
        "table_name": "RAW_RFQ_RESPONSES",
        "view_name": "RFQ_RESPONSES",
        "fields": {
            "rfq": "VARCHAR",
            "supplier": "VARCHAR",
            "price": "NUMBER",
            "currency": "VARCHAR",
            "deliveryTime": "VARCHAR",
            "additionalNotes": "VARCHAR",
            "files": "VARIANT",
            "status": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ"
        }
    },
    "rms": {
        "table_name": "RAW_RMS",
        "view_name": "RMS",
        "fields": {
            "name": "VARCHAR",
            "email": "VARCHAR",
            "phone": "VARCHAR",
            "assigned_suppliers": "VARIANT",
            "status": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "rmdocuments": {
        "table_name": "RAW_RM_DOCUMENTS",
        "view_name": "RM_DOCUMENTS",
        "fields": {
            "supplier": "VARCHAR",
            "rm": "VARCHAR",
            "title": "VARCHAR",
            "type": "VARCHAR",
            "url": "VARCHAR",
            "uploadedAt": "TIMESTAMP_TZ",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "rminvoices": {
        "table_name": "RAW_RM_INVOICES",
        "view_name": "RM_INVOICES",
        "fields": {
            "supplier": "VARCHAR",
            "rm": "VARCHAR",
            "invoiceNumber": "VARCHAR",
            "amount": "NUMBER",
            "currency": "VARCHAR",
            "status": "VARCHAR",
            "date": "TIMESTAMP_TZ",
            "dueDate": "TIMESTAMP_TZ",
            "description": "VARCHAR",
            "url": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "rmissues": {
        "table_name": "RAW_RM_ISSUES",
        "view_name": "RM_ISSUES",
        "fields": {
            "supplier": "VARCHAR",
            "rm": "VARCHAR",
            "title": "VARCHAR",
            "description": "VARCHAR",
            "status": "VARCHAR",
            "priority": "VARCHAR",
            "type": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "rmmeetings": {
        "table_name": "RAW_RM_MEETINGS",
        "view_name": "RM_MEETINGS",
        "fields": {
            "supplier": "VARCHAR",
            "rm": "VARCHAR",
            "title": "VARCHAR",
            "agenda": "VARCHAR",
            "date": "TIMESTAMP_TZ",
            "duration": "NUMBER",
            "link": "VARCHAR",
            "status": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "rmtasks": {
        "table_name": "RAW_RM_TASKS",
        "view_name": "RM_TASKS",
        "fields": {
            "supplier": "VARCHAR",
            "rm": "VARCHAR",
            "title": "VARCHAR",
            "description": "VARCHAR",
            "status": "VARCHAR",
            "priority": "VARCHAR",
            "dueDate": "TIMESTAMP_TZ",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    },
    "suppliers": {
        "table_name": "RAW_SUPPLIERS",
        "view_name": "SUPPLIERS",
        "fields": {
            "businessEmail": "VARCHAR",
            "password": "VARCHAR",
            "title": "VARCHAR",
            "firstName": "VARCHAR",
            "lastName": "VARCHAR",
            "jobTitle": "VARCHAR",
            "companyNameLocal": "VARCHAR",
            "companyNameword": "VARCHAR",
            "country": "VARCHAR",
            "state": "VARCHAR",
            "city": "VARCHAR",
            "taxId": "VARCHAR",
            "businessLocation": "VARCHAR",
            "businessType": "VARCHAR",
            "businessName": "VARCHAR",
            "registrationNumber": "VARCHAR",
            "address": "VARIANT",
            "phone": "VARCHAR",
            "contactPerson": "VARIANT",
            "payment": "VARIANT",
            "documents": "VARIANT",
            "storeInformation": "VARIANT",
            "confirmations": "VARIANT",
            "verificationStatus": "VARCHAR",
            "supplierId": "VARCHAR",
            "paymentStatus": "VARCHAR",
            "plan": "VARCHAR",
            "paymentPlan": "VARCHAR",
            "planPrice": "NUMBER",
            "preferredCurrency": "VARCHAR",
            "planExpiryDate": "TIMESTAMP_TZ",
            "isApprovalSeen": "BOOLEAN",
            "paymentOrderId": "VARCHAR",
            "paymentId": "VARCHAR",
            "lastPaymentReminder": "TIMESTAMP_TZ",
            "resetPasswordOTP": "VARCHAR",
            "resetPasswordOTPExpires": "TIMESTAMP_TZ",
            "passwordResetAttempts": "VARIANT",
            "inviteToken": "VARCHAR",
            "orders": "VARIANT",
            "rmPermissions": "VARIANT",
            "rmChangeRequest": "VARIANT"
        }
    },
    "supporttickets": {
        "table_name": "RAW_SUPPORT_TICKETS",
        "view_name": "SUPPORT_TICKETS",
        "fields": {
            "supportTicketId": "VARCHAR",
            "userId": "VARCHAR",
            "issue": "VARCHAR",
            "channel": "VARCHAR",
            "status": "VARCHAR",
            "resolution": "VARCHAR",
            "priority": "VARCHAR",
            "comments": "VARIANT",
            "attachments": "VARIANT",
            "attachmentRequests": "VARIANT",
            "history": "VARIANT",
            "customerSatisfaction": "NUMBER",
            "tags": "VARIANT",
            "dueDate": "TIMESTAMP_TZ",
            "escalated": "BOOLEAN",
            "chatSessionId": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ"
        }
    },
    "users": {
        "table_name": "RAW_USERS",
        "view_name": "USERS",
        "fields": {
            "firstName": "VARCHAR",
            "lastName": "VARCHAR",
            "email": "VARCHAR",
            "password": "VARCHAR",
            "role": "VARCHAR",
            "createdAt": "TIMESTAMP_TZ",
            "updatedAt": "TIMESTAMP_TZ"
        }
    }
}
