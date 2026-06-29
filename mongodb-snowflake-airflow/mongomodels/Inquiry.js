const mongoose = require("mongoose");

const inquirySchema = new mongoose.Schema({
  _id: { type: String },
  supplier: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Supplier",
    required: true,
  },
  product: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Product",
    required: true,
  },
  unit: { type: String, required: true },
  quantity: { type: Number, required: true },
  content: { type: String, required: true },
  email: { type: String, required: true },
  productImg: { type: String },
  files: [{ type: String }], // Cloudinary URLs
  createdAt: { type: Date, default: Date.now },
});

module.exports = mongoose.model("UsersInquiry", inquirySchema);
