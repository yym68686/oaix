export const state = {
  token: {
    page: 1,
    limit: 100,
    total: 0,
    query: "",
    status: "",
    sort: "-created_at",
    items: [],
    selected: new Set(),
    mode: "keys",
  },
  import: {
    queuePosition: "front",
  },
  settings: {
    items: [],
  },
};

export function tokenOffset() {
  return (state.token.page - 1) * state.token.limit;
}

export function sortParam(value) {
  switch (value) {
    case "created_at":
      return "oldest";
    case "-last_used_at":
      return "last_used";
    case "status":
      return "available";
    default:
      return "";
  }
}
