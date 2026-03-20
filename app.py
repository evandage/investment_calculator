import streamlit as st

st.title("📊 定投计算器")

# 输入
rmb = st.number_input("每月投入（人民币）", value=5000.0)
fx = st.number_input("汇率（USD/CNY）", value=6.9)

st.subheader("输入价格")

voo_price = st.number_input("VOO价格", value=400.0)
tlt_price = st.number_input("TLT价格", value=90.0)
gld_price = st.number_input("黄金GLD价格", value=180.0)

hs300_price = st.number_input("沪深300价格", value=4.0)
zz500_price = st.number_input("中证500价格", value=6.0)

if st.button("计算"):
    # 比例
    weights_us = {"VOO": 0.4, "TLT": 0.2, "GLD": 0.1}
    weights_cn = {"沪深300": 0.2, "中证500": 0.1}

    us_ratio = sum(weights_us.values())

    usd_total = (rmb * us_ratio) / fx

    st.subheader("📈 投资结果")

    # 美股
    st.write("### 美股")
    voo_usd = usd_total * (0.4 / us_ratio)
    tlt_usd = usd_total * (0.2 / us_ratio)
    gld_usd = usd_total * (0.1 / us_ratio)

    st.write(f"VOO：{voo_usd:.2f} USD → {voo_usd/voo_price:.3f} 股")
    st.write(f"TLT：{tlt_usd:.2f} USD → {tlt_usd/tlt_price:.3f} 股")
    st.write(f"GLD：{gld_usd:.2f} USD → {gld_usd/gld_price:.3f} 股")

    # A股
    st.write("### A股")

    hs300_amount = rmb * 0.2
    zz500_amount = rmb * 0.1

    hs300_lots = int((hs300_amount / hs300_price) // 100)
    zz500_lots = int((zz500_amount / zz500_price) // 100)

    st.write(f"沪深300：{hs300_lots*100} 股（{hs300_lots} 手）")
    st.write(f"中证500：{zz500_lots*100} 股（{zz500_lots} 手）")