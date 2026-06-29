const mongoose = require("mongoose");

const faqSchema = new mongoose.Schema({
  question: {
    type: String,
    required: true,
    unique: true,
  },
  answer: {
    type: String,
    required: true,
  },
  keywords: [String],
  category: {
    type: String,
    default: "General",
  },
  viewCount: {
    type: Number,
    default: 0,
  },
  ratings: [
    {
      userId: mongoose.Schema.Types.ObjectId,
      rating: { type: Number, min: 1, max: 5 },
    },
  ],
  relatedTickets: [
    { type: mongoose.Schema.Types.ObjectId, ref: "SupportTicket" },
  ],
  createdAt: { type: Date, default: Date.now },
});

module.exports = mongoose.model("FAQ", faqSchema);
