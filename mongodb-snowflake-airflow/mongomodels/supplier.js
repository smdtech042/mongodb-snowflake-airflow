const mongoose = require("mongoose");
const Counter = require("./Counter");

const supplierSchema = new mongoose.Schema({
  businessEmail: { type: String, required: true },
  password: { type: String, required: true },
  title: { type: String, required: true },
  firstName: { type: String, required: true },
  lastName: { type: String, required: true },
  jobTitle: { type: String, required: true },
  companyNameLocal: { type: String, required: true },
  companyNameword: { type: String, required: true },
  country: { type: String, required: true },
  state: { type: String, required: true },
  city: { type: String, required: true },
  taxId: {
    type: String,
  },
  businessLocation: { type: String },
  businessType: {
    type: String,
    enum: [
      "state-owned",
      "publicly-listed",
      "privately-owned",
      "charity",
      "proprietorship",
      null,
    ],
  },
  businessName: { type: String },
  registrationNumber: { type: String },
  address: {
    line1: { type: String },
    line2: { type: String },
    city: { type: String },
    state: { type: String },
    zip: { type: String },
    country: { type: String },
  },
  phone: { type: String, required: true },
  contactPerson: [
    {
      firstName: { type: String },
      middleName: { type: String },
      lastName: { type: String },
      citizenship: { type: String },
      birthDate: { type: String },
      birthCountry: { type: String },
      identityType: { type: String },
      identityNumber: { type: String },
    },
  ],
  payment: {
    cardNumber: { type: String },
    expiryMonth: { type: String },
    expiryYear: { type: String },
    cardholderName: { type: String },
  },
  documents: [
    {
      type: {
        type: String,
        enum: [
          "passport",
          "drivingLicense",
          "voterCard",
          "panCard",
          "aadharCard",
          "bankStatement",
          "utilityBill",
          "taxDocument",
          "gst",
          "tdsCertificate",
          "incomeTaxReturn",
          "importExport",
          null,
        ],
      },
      url: { type: String },
      uploadedOn: { type: Date },
    },
  ],
  storeInformation: [
    {
      storeName: { type: String },
      hasUPCs: { type: Boolean },
      ownsBrand: {
        type: String,
        enum: ["yes", "no", "some", null],
      },
    },
  ],
  confirmations: [
    {
      isBeneficialOwner: { type: Boolean },
      isLegalRepresentative: { type: Boolean },
      isOnlyOwner: { type: Boolean },
      agreeToTerms: { type: Boolean },
    },
  ],
  verificationStatus: {
    type: String,
    enum: ["formNotFilled", "pending", "approved", "rejected", "suspend"],
    default: "formNotFilled",
    required: true,
  },
  supplierId: { type: String, unique: true },
  paymentStatus: {
    type: String,
    enum: ["not_required", "pending", "completed"],
    default: "not_required",
  },
  plan: {
    type: String,
    enum: ["free", "paid"],
    default: "free",
  },
  paymentPlan: { type: String, default: "free" },
  planPrice: { type: Number, default: 0 },
  preferredCurrency: {
    type: String,
    enum: ["INR", "USD", "GBP", "EUR"],
    default: "INR",
  },
  planExpiryDate: { type: Date },
  isApprovalSeen: {
    type: Boolean,
    default: false,
  },
  paymentOrderId: { type: String },
  paymentId: { type: String },
  lastPaymentReminder: { type: Date },
  resetPasswordOTP: {
    type: String,
  },
  resetPasswordOTPExpires: {
    type: Date,
  },
  passwordResetAttempts: [
    {
      ip: { type: String },
      timestamp: { type: Date, default: Date.now },
    },
  ],
  inviteToken: { type: String },
  orders: [{ type: mongoose.Schema.Types.ObjectId, ref: "Order" }],
  rmPermissions: {
    orders: { type: Boolean, default: false },
    products: { type: Boolean, default: false },
    financials: { type: Boolean, default: false },
    documents: { type: Boolean, default: false },
    support: { type: Boolean, default: false },
  },
  rmChangeRequest: {
    status: {
      type: String,
      enum: ["none", "pending", "resolved"],
      default: "none",
    },
    reason: { type: String },
    requestedAt: { type: Date },
  },
});

supplierSchema.pre("validate", async function () {
  if (!this.supplierId) {
    const counter = await Counter.findOneAndUpdate(
      { name: "supplierId" },
      { $inc: { count: 1 } },
      { new: true, upsert: true },
    );
    this.supplierId = `OrazoSup${String(counter.count).padStart(3, "0")}`;
  }
});

supplierSchema.index({ businessEmail: 1 });
supplierSchema.index({ verificationStatus: 1 });

const Supplier =
  mongoose.models.Supplier || mongoose.model("Supplier", supplierSchema);

module.exports = Supplier;
