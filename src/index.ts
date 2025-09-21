import {
  BatchGetItemCommand,
  DynamoDBClient,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";
import {
  InvokeCommand,
  LambdaClient,
  UpdateFunctionConfigurationCommand,
} from "@aws-sdk/client-lambda";
import { marshall } from "@aws-sdk/util-dynamodb";

interface SearchInput {
  query: string;
  category: string;
  min_price: number;
  store: string;
}

interface SearchItemData {
  id: string;
  item_type: string;
  created: number;
}

interface ItemData {
  id: string;
  seller: {
    id: number;
    num_sell_items: number;
    ratings: {
      good: number;
    };
    num_ratings: number;
  };
  status: string;
  name: string;
  price: number;
  description: string;
  photos: string[];
  item_category_ntiers: {
    id: number;
    name: string;
  };
  parent_categories_ntiers: [
    {
      id: number;
      name: string;
    }
  ];
  item_condition: {
    id: number;
    name: string;
    subname: string;
  };
  shipping_payer: {
    id: number;
    name: string;
    code: string;
  };
  shipping_method: {
    id: number;
    name: string;
    is_deprecated: string;
  };
  shipping_from_area: {
    id: number;
    name: string;
  };
  shipping_duration: {
    id: number;
    name: string;
    min_days: number;
    max_days: number;
  };
  item_brand?: { id: number; name: string; sub_name: string };
  num_likes: number;
  num_comments: number;
  updated: number;
  created: number;
  auction_info?: Record<string, unknown>;
}

interface ItemImages {
  r2ImageUrls: string[];
  base64Images: string[];
}

interface IsEligible {
  isEligible: boolean;
}

interface AiCheck {
  blocked: boolean;
  isAllPassed: boolean;
}

interface AiCreate {
  blocked: boolean;
  shipping_weight_and_box_dimensions: {
    weight: number;
    box_dimensions: {
      length: number;
      width: number;
      height: number;
    };
  };
  information_for_ebay_listing: {
    listing_title_for_ebay_listing: string;
    item_condition_description_for_ebay_listing: string;
    item_specifics_for_ebay_listing: Record<string, any>;
    promotional_text_for_ebay_listing: string;
  };
}

interface AiShortenTitle {
  title: string;
}

interface AiChooseStore {
  store: string;
}

interface OfferPart {
  pricingSummary: {
    price: { currency: "USD"; value: string };
  };
  listingPolicies: {
    fulfillmentPolicyId: string;
    paymentPolicyId: string;
    returnPolicyId: string;
    bestOfferTerms: {
      bestOfferEnabled: false;
    };
  };
}

interface EbayListResult {
  listingId: string;
}

const DEPLOY_ENV = process.env.DEPLOY_ENV!;
const TABLE_NAME = process.env.TABLE_NAME!;
const LAMBDA_GET_SEARCH = process.env.LAMBDA_GET_SEARCH!;
const LAMBDA_MERC_SEARCH = process.env.LAMBDA_MERC_SEARCH!;
const LAMBDA_MERC_ITEM = process.env.LAMBDA_MERC_ITEM!;
const LAMBDA_IS_ELIGIBLE_FOR_LISTING =
  process.env.LAMBDA_IS_ELIGIBLE_FOR_LISTING!;
const LAMBDA_IMAGE_PROCESSOR = process.env.LAMBDA_IMAGE_PROCESSOR!;
// const LAMBDA_AI_CHECK = process.env.LAMBDA_AI_CHECK!;
const LAMBDA_AI_CHECK_GPT = process.env.LAMBDA_AI_CHECK_GPT!;
const LAMBDA_AI_CREATE = process.env.LAMBDA_AI_CREATE!;
const LAMBDA_AI_CREATE_GPT = process.env.LAMBDA_AI_CREATE_GPT!;
const LAMBDA_SHORTEN_TITLE = process.env.LAMBDA_SHORTEN_TITLE!;
const LAMBDA_AI_CHOOSE_STORE = process.env.LAMBDA_AI_CHOOSE_STORE!;
const LAMBDA_OFFER_PART = process.env.LAMBDA_OFFER_PART!;
const LAMBDA_EBAY_LIST = process.env.LAMBDA_EBAY_LIST!;

let SIGTERM_RECEIVED = false;
process.on("SIGTERM", () => {
  console.log("SIGTERM received");
  SIGTERM_RECEIVED = true;
});

let mercApiRunAt = 0; // Timestamp of the last Mercari API run
let listedAt = 0; // Timestamp of the last item listed

const ddbClient = new DynamoDBClient();
const lambdaClient = new LambdaClient();

function isOver24HoursAgo(pastUnixTimeInSeconds: number) {
  const nowInSeconds = Math.floor(Date.now() / 1000); // 現在時刻（秒単位）
  const diffInSeconds = nowInSeconds - pastUnixTimeInSeconds;

  return diffInSeconds >= 24 * 60 * 60; // 24時間 = 86400秒
}

export async function updateFunction(functionName: string) {
  const cmd = new UpdateFunctionConfigurationCommand({
    FunctionName: functionName,
    Description: `${Math.random()}`,
  });
  const response = await lambdaClient.send(cmd);
}

const makeDbArg = (
  toUpdate: Record<string, unknown>,
  noUpdate: Record<string, unknown>
) => {
  const res = Object.entries({ ...toUpdate, ...noUpdate }).reduce(
    (acc, [key, val], i) => {
      return {
        ExpressionAttributeNames: {
          ...acc.ExpressionAttributeNames,
          [`#n${i}`]: key,
        },
        ExpressionAttributeValues: {
          ...acc.ExpressionAttributeValues,
          [`:v${i}`]: val,
        },
        UpdateExpression:
          acc.UpdateExpression +
          (key in noUpdate
            ? `#n${i} = if_not_exists(#n${i}, :v${i}), `
            : `#n${i} = :v${i}, `),
      };
    },
    {
      ExpressionAttributeNames: {} as Record<string, string>,
      ExpressionAttributeValues: {} as Record<string, any>,
      UpdateExpression: "SET ",
    }
  );
  res.ExpressionAttributeValues = marshall(res.ExpressionAttributeValues);
  res.UpdateExpression = res.UpdateExpression.slice(0, -2);
  return res;
};

const makeDbInput = (ebaySku: string, attrs: Record<string, unknown>) => {
  const toUpdate = {
    ...attrs,
    isDraft: false,
    createdAt: getFormattedDate(new Date()),
    isImageChanged: false,
    isTitleChanged: false,
    isListed: true,
    isListedGsi: 1,
    isOrgLive: true,
    scanCount: 0,
  };

  const noUpdate = {};

  return {
    TableName: process.env.TABLE_NAME!,
    Key: {
      id: { S: `ITEM#naoto#${ebaySku}` },
    },
    ...makeDbArg(toUpdate, noUpdate),
  };
};

const runLambda = async (
  functionName: string,
  payload: Record<string, any>
) => {
  const command = new InvokeCommand({
    FunctionName: functionName,
    Payload: JSON.stringify(payload),
  });
  const response = await lambdaClient.send(command);
  const res_text = new TextDecoder("utf-8").decode(response.Payload);
  if (JSON.parse(res_text).errorMessage) {
    throw new Error(res_text);
  }
  const res_obj = JSON.parse(res_text);
  if (!res_obj.success) {
    throw new Error(`Lambda function ${functionName} failed.`);
  }
  return res_obj.result;
};

async function waitForMercApiRun(lastRunAt: number) {
  const elapsedTime = Date.now() - lastRunAt;
  const randomTime = Math.floor(Math.random() * 2000) + 9000;
  if (elapsedTime < randomTime) {
    console.log(`waitForMercApiRun. sleep for ${randomTime - elapsedTime}ms`);
    await new Promise((resolve) =>
      setTimeout(resolve, randomTime - elapsedTime)
    );
  }
}

async function waitForEbayList(lastRunAt: number) {
  const elapsedTime = Date.now() - lastRunAt;
  const randomTime = 15000;
  if (elapsedTime < randomTime) {
    console.log(`waitForEbayList. sleep for ${randomTime - elapsedTime}ms`);
    await new Promise((resolve) =>
      setTimeout(resolve, randomTime - elapsedTime)
    );
  }
}

async function filterSearchItems(items: SearchItemData[]) {
  const batchSize = 100;
  const filteredItems: SearchItemData[] = [];

  // Process items in batches of 100
  for (let i = 0; i < items.length; i += batchSize) {
    const batchItems = items.slice(i, i + batchSize);
    const batchIds = batchItems.map((item) => `ITEM#naoto#merc-${item.id}`);

    const result = await ddbClient.send(
      new BatchGetItemCommand({
        RequestItems: {
          [TABLE_NAME]: {
            Keys: batchIds.map((id) => ({ id: { S: id } })),
          },
        },
      })
    );

    const existingIds = new Set(
      (result.Responses?.[TABLE_NAME] || [])
        .filter(
          (item) =>
            item.isDraft?.BOOL ||
            (!item.isImageChanged?.BOOL && !item.isTitleChanged?.BOOL)
        )
        .map((item) => item.id.S)
    );
    const filteredBatch = batchItems.filter(
      (item) => !existingIds.has(`ITEM#naoto#merc-${item.id}`)
    );
    filteredItems.push(...filteredBatch);
  }
  return filteredItems;
}

const getFormattedDate = (date: Date): string => {
  const options: Intl.DateTimeFormatOptions = {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: "Asia/Tokyo",
  };
  return date.toLocaleString("ja-JP", options).replaceAll("/", "-");
};

const registerBannedItem = async (itemId: string) => {
  const command = new UpdateItemCommand({
    TableName: TABLE_NAME,
    Key: {
      id: { S: `ITEM#naoto#merc-${itemId}` },
    },
    UpdateExpression:
      "set isDraft = :isDraft, createdAt = :createdAt, orgUrl = :orgUrl, orgPlatform = :orgPlatform",
    ExpressionAttributeValues: {
      ":isDraft": { BOOL: true },
      ":createdAt": { S: getFormattedDate(new Date()) },
      ":orgUrl": { S: `https://jp.mercari.com/item/${itemId}` },
      ":orgPlatform": { S: "merc" },
    },
  });
  await ddbClient.send(command);
};

const isPackageTooBig = (shipping: {
  weight: number;
  box_dimensions: {
    length: number;
    width: number;
    height: number;
  };
}) => {
  const { length, width, height } = shipping.box_dimensions;
  const fedexVolume = (width * height * length) / 5000;
  if (
    shipping.weight > 8000 ||
    fedexVolume > 12 ||
    Math.max(width, height, length) > 80
  ) {
    return true;
  }
  return false;
};

function convertItemDataToParam(itemData: ItemData) {
  return {
    isAuction: !!itemData.auction_info,
    bidCount: itemData.auction_info?.total_bids || 0,
    likeCount: itemData.num_likes,
    isPayOnDelivery: itemData.shipping_payer.id !== 2,
    // rateScore: itemData.seller.star_rating_score,
    rateCount: itemData.seller.num_sell_items,
    // itemCategory: [itemData.item_category.name],
    itemCategory: [
      ...itemData.parent_categories_ntiers.map((c) => c.name),
      itemData.item_category_ntiers.name,
    ],
    brand: itemData.item_brand?.name || "",
    // brand: "",
    itemCondition: itemData.item_condition.name,
    shippedFrom: itemData.shipping_from_area.name,
    shippingMethod: itemData.shipping_method.name,
    shippedWithin: itemData.shipping_duration.name,
    sellerId: `/user/profile/${itemData.seller.id}`,
    lastUpdated: "X",
    created: itemData.created,
    updated: itemData.updated,
  };
}

async function doSearchItem(searchItem: SearchItemData, store: string) {
  console.log(`Processing item: ${searchItem.id}`);

  await waitForMercApiRun(mercApiRunAt);
  let item: ItemData;
  try {
    item = await runLambda(LAMBDA_MERC_ITEM, {
      id: searchItem.id,
    });
  } catch (error) {
    console.log("Merc API item failed.");
    await updateFunction(LAMBDA_MERC_ITEM);
    await new Promise((resolve) => setTimeout(resolve, 10000));
    return false;
  }
  mercApiRunAt = Date.now();

  if (item.status !== "on_sale") {
    console.log("Item is removed or sold out");
    return false;
  }

  const isEligibleResult: IsEligible = await runLambda(
    LAMBDA_IS_ELIGIBLE_FOR_LISTING,
    {
      item,
    }
  );
  if (!isEligibleResult.isEligible) {
    console.log("Item is not eligible for listing");
    await registerBannedItem(item.id);
    return false;
  }

  const itemImages: ItemImages = await runLambda(LAMBDA_IMAGE_PROCESSOR, {
    id: item.id,
    imageUrls: item.photos,
  });

  const aiCheckResult: AiCheck = await runLambda(LAMBDA_AI_CHECK_GPT, {
    thumbnailBase64: itemImages.base64Images[0],
    item,
  });
  if (!aiCheckResult.isAllPassed) {
    console.log("AI check result: Excluded");
    await registerBannedItem(item.id);
    return false;
  }

  let aiCreateResult: AiCreate = await runLambda(LAMBDA_AI_CREATE, {
    imagesBase64: itemImages.base64Images,
    item,
  });
  if (aiCreateResult.blocked) {
    aiCreateResult = await runLambda(LAMBDA_AI_CREATE_GPT, {
      imagesBase64: itemImages.base64Images,
      item,
    });
  }

  if (isPackageTooBig(aiCreateResult.shipping_weight_and_box_dimensions)) {
    console.log("Package is too big for shipping");
    await registerBannedItem(item.id);
    return false;
  }

  if (
    aiCreateResult.information_for_ebay_listing.listing_title_for_ebay_listing
      .length > 80
  ) {
    console.log("Title is too long for eBay listing");
    const shortenedTitleResult: AiShortenTitle = await runLambda(
      LAMBDA_SHORTEN_TITLE,
      {
        id: item.id,
        title:
          aiCreateResult.information_for_ebay_listing
            .listing_title_for_ebay_listing,
      }
    );
    aiCreateResult.information_for_ebay_listing.listing_title_for_ebay_listing =
      shortenedTitleResult.title;
  }

  let storeResult = store;
  if (storeResult === "X") {
    console.log("choosing store...");
    let aiChooseStoreResult: AiChooseStore = await runLambda(
      LAMBDA_AI_CHOOSE_STORE,
      {
        title:
          aiCreateResult.information_for_ebay_listing
            .listing_title_for_ebay_listing,
      }
    );
    storeResult = aiChooseStoreResult.store;
  }
  console.log({ storeResult });

  if (storeResult !== "A" && storeResult !== "B") {
    console.error(`ERROR storeResult is invalid. ${storeResult}`);
  }

  const account = (() => {
    if (DEPLOY_ENV === "dev") return "test";
    if (storeResult === "A") return "main";
    return "sub";
  })();

  const username = (() => {
    if (DEPLOY_ENV === "dev") return "test";
    if (storeResult === "A") return "naoto";
    return "sub";
  })();

  console.log({ account, username });

  const dbData = {
    orgPlatform: "merc",
    orgUrl: `https://jp.mercari.com/item/${item.id}`,
    orgTitle: item.name,
    orgPrice: item.price,
    orgImageUrls: item.photos,
    orgExtraParam: convertItemDataToParam(item),
    ebaySku: `merc-${item.id}`,
    ebayImageUrls: itemImages.r2ImageUrls,
    username,
    weightGram: aiCreateResult.shipping_weight_and_box_dimensions.weight,
    boxSizeCm: [
      aiCreateResult.shipping_weight_and_box_dimensions.box_dimensions.length,
      aiCreateResult.shipping_weight_and_box_dimensions.box_dimensions.width,
      aiCreateResult.shipping_weight_and_box_dimensions.box_dimensions.height,
    ],
    ebayTitle:
      aiCreateResult.information_for_ebay_listing
        .listing_title_for_ebay_listing,
    ebayDescription: `<div style="color: rgb(51, 51, 51); font-family: Arial;"><p>${aiCreateResult.information_for_ebay_listing.promotional_text_for_ebay_listing}</p><h3 style="margin-top: 1.6em;">Condition</h3><p>${aiCreateResult.information_for_ebay_listing.item_condition_description_for_ebay_listing}</p><h3 style="margin-top: 1.6em;">Shipping</h3><p>Tracking numbers are provided to all orders. The item will be carefully packed to ensure it arrives safely.</p><h3 style="margin-top: 1.6em;">Customs and import charges</h3><p>Import duties, taxes, and charges are not included in the item price or shipping cost. Buyers are responsible for these charges. These charges may be collected by the carrier when you receive the item.</p></div>`,
    ebayCategory: "69528",
    ebayStoreCategory: "/Anime Merchandise",
    ebayCondition: "USED_EXCELLENT",
    ebayConditionDescription:
      aiCreateResult.information_for_ebay_listing
        .item_condition_description_for_ebay_listing,
    ebayAspectParam: Object.fromEntries([
      ...Object.entries(
        aiCreateResult.information_for_ebay_listing
          .item_specifics_for_ebay_listing
      )
        .filter(([_, value]) => {
          if (value == null || value === "") return false;
          if (typeof value === "string" && value.length > 65) return false;
          if (Array.isArray(value)) {
            return (
              value.length > 0 &&
              value.every((v) => v != null && v !== "" && v.length <= 65)
            );
          }
          return true;
        })
        .map(([key, value]) => [
          key,
          Array.isArray(value) ? value.slice(0, 30) : [value],
        ]),
      ["Country/Region of Manufacture", ["Japan"]],
    ]),
  };
  const dbInput = makeDbInput(dbData.ebaySku, dbData);
  // console.log(JSON.stringify(dbInput));
  await ddbClient.send(new UpdateItemCommand(dbInput));

  const inventoryPayload = {
    availability: {
      shipToLocationAvailability: {
        quantity: 1,
      },
    },
    condition: dbData.ebayCondition,
    product: {
      title: dbData.ebayTitle,
      description: dbData.ebayDescription,
      imageUrls: dbData.ebayImageUrls,
      aspects: dbData.ebayAspectParam,
    },
    ...(dbData.ebayConditionDescription
      ? { conditionDescription: dbData.ebayConditionDescription }
      : {}),
  };

  const offerPart: OfferPart = await runLambda(LAMBDA_OFFER_PART, {
    id: item.id,
    account,
    orgPrice: item.price,
    ...aiCreateResult.shipping_weight_and_box_dimensions,
  });

  const offerPayload = {
    sku: dbData.ebaySku,
    marketplaceId: "EBAY_US",
    format: "FIXED_PRICE",
    availableQuantity: 1,
    categoryId: dbData.ebayCategory,
    ...offerPart,
    merchantLocationKey: "main-warehouse",
    storeCategoryNames: [dbData.ebayStoreCategory],
  };

  await waitForEbayList(listedAt);
  const ebayListResult: EbayListResult = await runLambda(LAMBDA_EBAY_LIST, {
    sku: dbData.ebaySku,
    inventoryPayload,
    offerPayload,
    account,
  });
  listedAt = Date.now();
  console.log(
    `Item ${item.id} listed successfully with eBay listing ID: ${ebayListResult.listingId}`
  );
  return true;
}

async function main() {
  while (true) {
    if (SIGTERM_RECEIVED) {
      console.log("Loop of search ended because of SIGTERM.");
      break;
    }

    const searchInput: SearchInput = await runLambda(LAMBDA_GET_SEARCH, {});
    console.log(JSON.stringify({ searchInput }));
    // if (searchInput.store === "B") continue;

    await waitForMercApiRun(mercApiRunAt);
    let itemList: SearchItemData[];
    try {
      itemList = await runLambda(LAMBDA_MERC_SEARCH, searchInput);
    } catch (error) {
      console.log("Merc API search failed.");
      await updateFunction(LAMBDA_MERC_SEARCH);
      await new Promise((resolve) => setTimeout(resolve, 10000));
      continue;
    }
    mercApiRunAt = Date.now();

    // idが重複するものを除く（最初に出現したものを残す）
    const uniqueItemList = Array.from(
      new Map(itemList.map((item) => [item.id, item])).values()
    );

    // メルカリ商品かつ出品から１日経過のもののみにフィルタリング
    const itemListFiltered = uniqueItemList.filter((item) => {
      return (
        item.item_type === "ITEM_TYPE_MERCARI" && isOver24HoursAgo(item.created)
      );
    });

    // すでにDBに登録済みの商品を除外
    const finalItemList = await filterSearchItems(itemListFiltered);
    console.log(
      JSON.stringify({
        orgItemList: {
          count: itemList.length,
        },
        filteredItemList: {
          count: finalItemList.length,
          data: finalItemList,
        },
      })
    );

    // 商品リストから商品を登録するループを実行
    let listedCount = 0;
    for (const item of finalItemList) {
      if (listedCount >= 10) {
        console.log("Reached the limit of 10 items listed in this search.");
        break;
      }
      if (SIGTERM_RECEIVED) {
        console.log("Loop of item ended because of SIGTERM.");
        break;
      }
      const isListed = await doSearchItem(item, searchInput.store);
      if (isListed) {
        listedCount++;
      }
    }
  }
}

main()
  .then(() => {
    console.log(
      JSON.stringify({
        message: "Container ended.",
      })
    );
  })
  .catch((error) => {
    console.error(
      JSON.stringify({
        message: "Container accidently ended.",
        content: {
          name: error.name,
          message: error.message,
          stack: error.stack,
        },
      })
    );
  });
