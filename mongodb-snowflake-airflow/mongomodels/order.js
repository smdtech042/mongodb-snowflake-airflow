const mongoose = require("mongoose");

const orderSchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: "ClientUser" },
    products: [
      {
        productId: { type: String, required: true },
        quantity: { type: Number, required: true },
      },
    ],
    total: { type: Number, required: true },
    status: {
      type: String,
      enum: ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"],
      default: "Pending",
    },
    address: {
      fullName: { type: String, required: true },
      phoneNumber: { type: String, required: true },
      streetAddress: { type: String, required: true },
      apartment: { type: String },
      locality: { type: String },
      country: { type: String, required: true },
      state: { type: String, required: true },
      city: { type: String, required: true },
      zipCode: { type: String, required: true },
    },
    orderId: { type: String, unique: true, required: true },
    paymentId: { type: String },
    discountCode: { type: String },
    trackingUpdates: [
      {
        status: { type: String, required: true },
        timestamp: { type: Date, default: Date.now },
        notes: { type: String },
      },
    ],
    isGuest: { type: Boolean, default: false },
    guestEmail: { type: String },
    deliveryPartner: { type: String, enum: ["Shiprocket", "FedEx"] },
    supplierApprovalStatus: {
      type: String,
      enum: ["Pending", "Accepted", "Rejected"],
      default: "Pending",
    },
    supplier: { type: mongoose.Schema.Types.ObjectId, ref: "Supplier" },
    shipmentId: { type: String },
    trackingNumber: { type: String },
    paymentStatus: {
      type: String,
      enum: ["Pending", "Success", "Failed"],
      default: "Pending",
    },
    invoiceUrl: { type: String },
  },
  { timestamps: true }
);

module.exports = mongoose.model("Order", orderSchema);
