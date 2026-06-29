const mongoose = require("mongoose");
const Counter = require("./Counter");

const inquirySchema = new mongoose.Schema({
  inquiryId: {
    type: String,
    required: true,
    unique: true,
  },
  name: {
    type: String,
    required: true,
  },
  email: {
    type: String,
    required: true,
    lowercase: true,
    trim: true,
  },
  contact: {
    type: String,
    required: true,
  },
  topic: {
    type: String,
    required: true,
  },
  message: {
    type: String,
    required: true,
  },
  status: {
    type: String,
    enum: ["pending", "resolved", "in-progress"],
    default: "pending",
  },
  createdAt: {
    type: Date,
    default: Date.now,
  },
  updatedAt: {
    type: Date,
    default: Date.now,
  },
});

// Pre-save hook to generate the inquiryId before saving
inquirySchema.pre("validate", async function (next) {
  // If the inquiryId isn't set already, generate one
  if (!this.inquiryId) {
    try {
      const counter = await Counter.findOneAndUpdate(
        { name: "inquiryId" },
        { $inc: { count: 1 } },
        { new: true, upsert: true }
      );

      // Assign a generated inquiryId with padded formatting
      this.inquiryId = `OraInQ${String(counter.count).padStart(3, "0")}`;
      next(); // Proceed with validation
    } catch (error) {
      next(error); // Pass any errors to the next middleware
    }
  } else {
    next(); // If inquiryId is already set, continue
  }
});

const Inquiry = mongoose.model("Inquiry", inquirySchema);

module.exports = Inquiry;
