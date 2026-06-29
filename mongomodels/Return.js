const mongoose = require("mongoose");

const returnSchema = new mongoose.Schema({
  orderId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Order",
    required: true,
  },
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "User",
  },
  supplierId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Supplier",
    required: true,
  },
  reason: { type: String, required: true },
  subReason: String,
  images: [String],
  status: {
    type: String,
    enum: ["Pending", "Accepted", "Rejected"],
    default: "Pending",
  },
  returnShipmentId: { type: String },
  returnTrackingNumber: { type: String },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
});

returnSchema.index({ supplierId: 1, status: 1 });
returnSchema.index({ orderId: 1 });

module.exports = mongoose.model("Return", returnSchema);
