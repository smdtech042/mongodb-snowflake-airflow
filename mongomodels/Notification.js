const mongoose = require("mongoose");

const notificationSchema = new mongoose.Schema(
  {
    recipient: {
      type: mongoose.Schema.Types.ObjectId,
      required: [true, "Recipient is required"],
      index: true,
      refPath: "recipientModel",
    },
    recipientModel: {
      type: String,
      required: [true, "Recipient model is required"],
      enum: ["ClientUser", "Supplier"],
    },
    eventType: {
      type: String,
      required: [true, "Event type is required"],
      enum: [
        "rfq_received",
        "rfq_follow_up",
        "account_created",
        "account_updated",
        "verification_status_changed",
        "order_placed",
        "order_updated",
        "payment_status_changed",
        "product_approved",
        "product_rejected",
        "document_upload",
        "custom",
      ],
      index: true,
    },
    message: {
      type: String,
      required: [true, "Message is required"],
      trim: true,
    },
    relatedEntity: {
      entityType: {
        type: String,
        enum: [
          "RFQ",
          "Order",
          "Product",
          "Supplier",
          "ClientUser",
          "Document",
          "none",
        ],
        required: [true, "Entity type is required"],
      },
      entityId: {
        type: mongoose.Schema.Types.ObjectId,
        refPath: "relatedEntity.entityType",
      },
    },
    details: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
    status: {
      type: String,
      enum: ["pending", "delivered", "read", "failed"],
      default: "pending",
      required: [true, "Status is required"],
      
    },
    deliveredAt: Date,
    readAt: Date,
    failureReason: String,
  },
  { timestamps: true }
);

// Define indexes once here
notificationSchema.index({ recipient: 1, eventType: 1 });
notificationSchema.index({ "relatedEntity.entityId": 1 });
notificationSchema.index({ status: 1 });

// Update updatedAt before saving
notificationSchema.pre("save", function (next) {
  this.updatedAt = Date.now();
  next();
});

const Notification =
  mongoose.models.Notification ||
  mongoose.model("Notification", notificationSchema);

module.exports = Notification;
