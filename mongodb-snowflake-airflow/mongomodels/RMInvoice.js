const mongoose = require("mongoose");

const invoiceSchema = new mongoose.Schema(
  {
    supplier: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Supplier",
      required: true,
    },
    rm: { type: mongoose.Schema.Types.ObjectId, ref: "RM", required: true },
    invoiceNumber: { type: String, required: true },
    amount: { type: Number, required: true },
    currency: { type: String, default: "INR" },
    status: {
      type: String,
      enum: ["pending", "paid", "overdue"],
      default: "pending",
    },
    date: { type: Date, default: Date.now },
    dueDate: { type: Date },
    description: { type: String },
    url: { type: String }, // Link to PDF
  },
  { timestamps: true },
);

module.exports = mongoose.model("RMInvoice", invoiceSchema);
