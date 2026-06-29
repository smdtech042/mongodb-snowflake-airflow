const mongoose = require("mongoose");

const inquiryResponseSchema = new mongoose.Schema({
  inquiry: {
    type: String,
    ref: "UsersInquiry",
    required: true,
  },
  supplier: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Supplier",
    required: true,
  },
  message: { type: String, required: true },
  price: { type: Number },
  currency: { type: String },
  deliveryTime: { type: String },
  additionalNotes: { type: String },
  files: [{ url: { type: String } }],
  status: {
    type: String,
    enum: ["pending", "accepted", "rejected"],
    default: "pending",
  },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
});

module.exports = mongoose.model("InquiryResponse", inquiryResponseSchema);
