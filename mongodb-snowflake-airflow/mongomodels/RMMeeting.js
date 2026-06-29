const mongoose = require("mongoose");

const meetingSchema = new mongoose.Schema(
  {
    supplier: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Supplier",
      required: true,
    },
    rm: { type: mongoose.Schema.Types.ObjectId, ref: "RM", required: true },
    title: { type: String, required: true },
    agenda: { type: String },
    date: { type: Date, required: true },
    duration: { type: Number, default: 30 }, // minutes
    link: { type: String },
    status: {
      type: String,
      enum: ["scheduled", "completed", "cancelled"],
      default: "scheduled",
    },
  },
  { timestamps: true },
);

module.exports = mongoose.model("RMMeeting", meetingSchema);
