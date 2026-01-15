import streamlit as st

def render(builder_engine, product_list):
    st.header("Smart Quote Builder 2.0")
    st.markdown("Leverage historical actuals to predict future effort.")
    
    selected_product = st.selectbox("Select Product Category", product_list)
    
    if selected_product:
        benchmarks = builder_engine.get_product_benchmarks(selected_product)
        
        if benchmarks.empty:
            st.warning("No historical data found for this product.")
        else:
            st.success(f"Found {len(benchmarks)} standard tasks for {selected_product}")
            
            st.dataframe(
                benchmarks.style.format({
                    "avg_hours": "{:.1f}",
                    "median_hours": "{:.1f}",
                    "risk_factor": "{:.2f}x"
                }),
                width="stretch"
            )
            
            st.markdown("""
            **Risk Factor Guide:**
            * **1.0x**: We deliver exactly what we quote.
            * **>1.0x**: We typically overrun (Under-quoting).
            * **<1.0x**: We deliver efficiently (Padding/Buffer).
            """)
            
