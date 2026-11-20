from . import fpa_financial, kgi, poems, tickrs, uob_kay_hian


SOURCES = {
    "uob_kay_hian": uob_kay_hian.list_documents,
    "poems": poems.list_documents,
    "kgi": kgi.list_documents,
    "fpa_financial": fpa_financial.list_documents,
    "tickrs": tickrs.list_documents,
}
