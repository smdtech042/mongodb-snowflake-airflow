// Product.js
const mongoose = require("mongoose");

const productSchema = new mongoose.Schema(
  {
    id: {
      type: String,
      required: true,
      unique: true,
    },
    category: {
      type: String,
      required: true,
    },
    subcategory: {
      type: String,
      required: true,
    },
    subsubcategory: {
      type: String,
      required: true,
    },
    productName: {
      type: String,
      required: true,
    },
    modelNumber: {
      type: String,
      required: true,
    },
    images: {
      type: [String],
      default: [],
    },
    thumbnailImage: {
      type: String,
      required: true,
    },
    price: {
      type: String,
      required: true,
    },
    stock: {
      type: Number,
      required: true,
      min: 1,
    },
    description: {
      type: String,
      required: true,
    },
    searchKeywords: {
      type: Object,
      required: true,
    },
    attributes: [
      {
        name: {
          type: String,
          required: true,
        },
        value: {
          type: mongoose.Schema.Types.Mixed, // Allow String or [String]
          required: true,
          validate: {
            validator: function (v) {
              return (
                typeof v === "string" ||
                (Array.isArray(v) &&
                  v.every((item) => typeof item === "string"))
              );
            },
            message: "Attribute value must be a string or an array of strings",
          },
        },
        type: {
          type: String,
          enum: ["predefined", "custom"],
          required: true,
        },
        groupId: {
          type: String,
          required: false,
        },
        optionId: {
          type: String,
          required: false,
        },
      },
    ],
    unitOfMeasure: {
      type: String,
      required: true,
    },
    paymentMethod: {
      type: String,
      required: true,
    },
    orderLeadTimeRange: {
      type: Object,
      required: true,
    },
    fobPrices: {
      type: [Object],
      default: [],
    },
    additionalInfo: {
      type: Object,
      default: {},
    },
    supplier: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Supplier",
      required: true,
    },
    listingStatus: {
      type: String,
      default: "active",
    },
    productStatus: {
      type: String,
      default: "pending",
    },
    isDeleted: {
      type: Boolean,
      default: false,
      index: true,
    },
    deletedAt: {
      type: Date,
      default: null,
    },
    isArchived: {
      type: Boolean,
      default: false,
      index: true,
    },
    archivedAt: {
      type: Date,
      default: null,
    },
  },
  {
    timestamps: true,
  }
);

productSchema.index({ supplier: 1, isDeleted: 1, isArchived: 1 });
productSchema.index({ supplier: 1, stock: 1 });
productSchema.index({ supplier: 1, isDeleted: 1, deletedAt: 1 });

productSchema.index(
  { productName: "text", modelNumber: "text", category: "text" },
  {
    weights: { productName: 10, modelNumber: 5, category: 1 },
    name: "ProductTextSearchIndex_v2"
  }
);

// Individual indexes for Regex optimization (Partial Match)
productSchema.index({ productName: 1 });
productSchema.index({ modelNumber: 1 });
productSchema.index({ category: 1 });

module.exports = mongoose.model("Product", productSchema);
