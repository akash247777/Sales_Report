import streamlit as st
import pyodbc
from datetime import datetime
from decimal import Decimal
import io
import zipfile
import pandas as pd
import concurrent.futures

# =============================================================================
# Utility Functions
# =============================================================================

def format_currency(value):
    """Format a numeric value as a currency string with 2 decimals."""
    return f"{value:,.2f}"

def format_report(result, site_id, site_name, from_date, to_date):
    """
    Process the query result (a list of tuples) and produce a text report
    in which every line is fixed to 180 characters. This ensures that the
    output file has constant dimensions.
    """
    PAGE_WIDTH = 180

    def fix_line(line, width=PAGE_WIDTH):
        clean = line.rstrip("\n")
        if len(clean) < width:
            return clean + " " * (width - len(clean))
        else:
            return clean[:width]

    now = datetime.now()
    header_date = now.strftime("%d/%m/%Y")
    header_time = now.strftime("%I:%M %p")

    lines = []
    lines.append(f"DATE: {header_date}".rjust(PAGE_WIDTH))
    lines.append(f"TIME: {header_time}".rjust(PAGE_WIDTH))
    lines.append("")
    lines.append("APOLLO PHARMACIES LIMITED".center(PAGE_WIDTH))
    lines.append(f"{site_id} - {site_name}".center(PAGE_WIDTH))
    lines.append("")
    lines.append("Sales Transaction Summary Report".center(PAGE_WIDTH))
    lines.append(f"From Date : {from_date}    To Date : {to_date}".center(PAGE_WIDTH))
    lines.append("-" * PAGE_WIDTH)

    header_groups = (
        "|" +
        " SALES ".center(55) +
        "|" +
        " RETURNS ".center(55) +
        "|" +
        " NET ".center(55) +
        "|"
    )
    lines.append(header_groups)
    lines.append("-" * PAGE_WIDTH)

    header_cols = (
        f"{'BILLTYPE':<17} |"
        f"{'NO':>8} |"
        f"{'AMT':>12} |"
        f"{'DISC':>12} |"
        f"{'NET':>12} |"
        f"{'NO':>6} |"
        f"{'AMT':>12} |"
        f"{'DISC':>12} |"
        f"{'NET':>12} |"
        f"{'NO':>6} |"
        f"{'AMT':>12} |"
        f"{'DISC':>12} |"
        f"{'NET':>12} |"
    )
    lines.append(header_cols)
    lines.append("-" * PAGE_WIDTH)

    sales_data = []
    partner_data = []

    for row in result:
        isheader = row[0]
        if isheader in (1, 3):
            sale_net = row[3]
            sale_disc = row[4]
            ret_net = row[5]
            ret_disc = row[6]
            sales_data.append({
                "BILLTYPE": row[2],
                "SALECOUNT": row[7],
                "SALE_NET": sale_net,
                "SALE_DISC": sale_disc,
                "SALE_AMT": sale_net + sale_disc,
                "RETCOUNT": row[8],
                "RET_NET": ret_net,
                "RET_DISC": ret_disc,
                "RET_AMT": ret_net + ret_disc
            })
        elif isheader == 0:
            partner_data.append({
                "NAME": row[2],
                "BILLCNT": row[7],
                "AMOUNT": row[3]
            })

    tot_sale_count = tot_sale_amt = tot_sale_disc = tot_sale_net = 0
    tot_ret_count = tot_ret_amt = tot_ret_disc = tot_ret_net = 0
    net_cash_sales = 0

    for s in sales_data:
        if s["BILLTYPE"].upper() != "GIFT":
            tot_sale_count   += s["SALECOUNT"]
            tot_sale_amt     += float(s["SALE_AMT"])
            tot_sale_disc    += float(s["SALE_DISC"])
            tot_sale_net     += float(s["SALE_NET"])
            tot_ret_count    += s["RETCOUNT"]
            tot_ret_amt      += float(s["RET_AMT"])
            tot_ret_disc     += float(s["RET_DISC"])
            tot_ret_net      += float(s["RET_NET"])
        if s["BILLTYPE"].upper() == "CASH":
            net_cash_sales = s["SALE_NET"] + s["RET_NET"]

    tot_overall_count = tot_sale_count + tot_ret_count
    tot_overall_amt   = tot_sale_amt + tot_ret_amt
    tot_overall_disc  = tot_sale_disc + tot_ret_disc
    tot_overall_net   = tot_sale_net + tot_ret_net

    for s in sales_data:
        overall_count = s["SALECOUNT"] + s["RETCOUNT"]
        overall_amt   = s["SALE_AMT"] + s["RET_AMT"]
        overall_disc  = s["SALE_DISC"] + s["RET_DISC"]
        overall_net   = s["SALE_NET"] + s["RET_NET"]
        row_line = (
            f"{s['BILLTYPE']:<17} |"
            f"{s['SALECOUNT']:8d} |"
            f"{format_currency(s['SALE_AMT']):>12} |"
            f"{format_currency(s['SALE_DISC']):>12} |"
            f"{format_currency(s['SALE_NET']):>12} |"
            f"{s['RETCOUNT']:6d} |"
            f"{format_currency(s['RET_AMT']):>12} |"
            f"{format_currency(s['RET_DISC']):>12} |"
            f"{format_currency(s['RET_NET']):>12} |"
            f"{overall_count:6d} |"
            f"{format_currency(overall_amt):>12} |"
            f"{format_currency(overall_disc):>12} |"
            f"{format_currency(overall_net):>12} |"
        )
        lines.append(row_line)
    lines.append("-" * PAGE_WIDTH)

    totals_line = (
        f"{'TOTALAMOUNT   :':<17} |"
        f"{tot_sale_count:8d} |"
        f"{format_currency(tot_sale_amt):>12} |"
        f"{format_currency(tot_sale_disc):>12} |"
        f"{format_currency(tot_sale_net):>12} |"
        f"{int(tot_ret_count):6d} |"
        f"{format_currency(tot_ret_amt):>12} |"
        f"{format_currency(tot_ret_disc):>12} |"
        f"{format_currency(tot_ret_net):>12} |"
        f"{int(tot_overall_count):6d} |"
        f"{format_currency(tot_overall_amt):>12} |"
        f"{format_currency(tot_overall_disc):>12} |"
        f"{format_currency(tot_overall_net):>12} |"
    )
    lines.append(totals_line)
    lines.append("-" * PAGE_WIDTH)

    lines.extend([
        "\nSALES :-",
        f"\n       Net Cash Sales        : {format_currency(net_cash_sales)}",
        "       Total Paid In         :       0.00",
        "       Total Paid out        :       0.00"
    ])
    total_paid_in = Decimal('0.0')
    total_paid_out = Decimal('0.0')
    total_sales = Decimal(net_cash_sales) + total_paid_in + total_paid_out
    lines.append(f"       Total Sales           : {format_currency(total_sales)}\n")
    
    lines.extend([
        "HealingCard Collections:",
        f"     Cash Collections        : {'0':>9}",
        f"     Credit Card Collections : {'0':>9}",
        f"     Total Collection        : {'0':>9}\n",
        f"Total Cash Amount            : {format_currency(total_sales)} ",
        "\n" + "-" * 180 + "\n"
    ])

    lines.append("\nPartner Program Summary  :\n")
    partner_header = " slno| Name                                     |     NoInv        |    Amount    |"
    lines.append(partner_header)
    lines.append("-" * PAGE_WIDTH)
    
    tot_partner_inv = tot_partner_amt = 0
    for idx, p in enumerate(partner_data, start=1):
        tot_partner_inv += p["BILLCNT"]
        tot_partner_amt += float(p["AMOUNT"])
        part_line = (
            f"{idx:6d} | {p['NAME']:<38} |     {p['BILLCNT']:12d} | {format_currency(p['AMOUNT']):>12} |"
        )
        lines.append(part_line)
    lines.append("-" * (PAGE_WIDTH - 50))
    partner_totals_line = (
        f"      TOTAL AMOUNT:                    {tot_partner_inv:27d} | {format_currency(tot_partner_amt):>9} |"
    )
    lines.append(partner_totals_line)
    lines.append("-" * (PAGE_WIDTH - 50))
    
    fixed_lines = [fix_line(line) for line in lines]
    return "\n".join(fixed_lines)

def try_connection(series, formatted_site_id, username, password, database):
    host = f"{series}{formatted_site_id}"
    try:
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host};"
            f"UID={username};"
            f"PWD={password};"
            f"DATABASE={database};"
        )
        return connection
    except pyodbc.Error:
        return None

def connect_to_database(site_id, username, password, database, ip_series_choice):
    formatted_site_id = f"{site_id[:3]}.{int(site_id[3:])}"
    if ip_series_choice == "16":
        ip_series = ["10.16."]
    elif ip_series_choice == "28":
        ip_series = ["10.28."]
    else:
        ip_series = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(ip_series)) as executor:
        futures = {executor.submit(try_connection, series, formatted_site_id, username, password, database): series for series in ip_series}
        for future in concurrent.futures.as_completed(futures):
            connection = future.result()
            if connection:
                return connection
    return None

@st.cache_data(show_spinner=False)
def get_report_data(site_id, from_date, to_date, username, password, database, ip_series_choice):
    connection = connect_to_database(site_id, username, password, database, ip_series_choice)
    if not connection:
        raise ConnectionError(f"Could not connect to any IP series for site {site_id}.")
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM ax.inventsite WHERE siteid = ?", site_id)
        site_row = cursor.fetchone()
        site_name = site_row[0] if site_row else "Unknown Site"
        
        query = """
        select ISHEADER = CASE WHEN BILLTYPE ='GIFT' THEN 3 ELSE 1 END,
               ACXCORPCODE = -1,
               upper(BILLTYPE) BILLTYPE,
               sum(saleamt) NETSALEAMT,
               Cast(sum(discamt) as decimal(12,2)) DISCAMT,
               Cast(sum(RETAMT) as decimal(12,2)) NETRETAMT,
               Cast(sum(RETDISC) as decimal(12,2)) RETDISC,
               sum(isscnt) SALECOUNT,
               sum(retcnt) RETCNT 
        from (
            select name billtype,
                   Cast(sum(AMOUNTTENDERED) as decimal(12,2)) saleamt,
                   sum(DISCAMOUNT) DISCAMT,
                   0 RETAMT,
                   0 RETDISC,
                   count(distinct rt.receiptid) isscnt,
                   0 retcnt 
            from ax.retailtransactiontable rt
            join ax.RETAILTRANSACTIONPAYMENTTRANS rpt
              on rt.TRANSACTIONID = rpt.TRANSACTIONID and rt.RECEIPTID = rpt.RECEIPTID
            join RETAILTENDERTYPETABLE rtt
              on rpt.TENDERTYPE = rtt.TENDERTYPEID
            where ENTRYSTATUS = 0  
              and acxtranstype = 0 
              and rpt.TRANSACTIONSTATUS = 0 
              and rpt.BUSINESSDATE between ? and ? 
              and rpt.RECEIPTID not in (
                    Select IQ.receiptid 
                    from ax.RETAILTRANSACTIONPAYMENTTRANS as IQ 
                    where IQ.receiptid like 'IP%' 
                      and IQ.tendertype in (1,2)
                      and IQ.BUSINESSDATE between ? and ?
              )
            group by name, DISCAMOUNT
            union  
            select name,
                   0 saleamt,
                   0 DISCAMT,
                   Sum(AMOUNTTENDERED) AMOUNTTENDERED,
                   sum(-1*DISCAMOUNT) DISCAMT,
                   0 isscnt,
                   count(distinct rt.receiptid) retcnt 
            from ax.retailtransactiontable rt
            join ax.RETAILTRANSACTIONPAYMENTTRANS rpt
              on rt.TRANSACTIONID = rpt.TRANSACTIONID and rt.RECEIPTID = rpt.RECEIPTID
            join RETAILTENDERTYPETABLE rtt
              on rpt.TENDERTYPE = rtt.TENDERTYPEID
            where ENTRYSTATUS = 0  
              and acxtranstype <> 0 
              and rpt.TRANSACTIONSTATUS = 0 
              and rpt.BUSINESSDATE between ? and ?  
            group by name 
        ) a 
        group by billtype
        union all
        select ISHEADER = 0,
               ACXCORPCODE,
               ax.getcorporatename(acxcorpcode) CORPORATE,
               (cast(sum(CASE WHEN ACXCORPCODE ='172' AND ACXCREDIT = 0 THEN 0 ELSE -1*GROSSAMOUNT END)
                - sum(case when ACXTRANSTYPE = 0 then discamount
                           when ACXTRANSTYPE <> 0 then -1*discamount end) as decimal(18,2)) - sum(ACXLOYALTY)) NETAMT,
               0, 0, 0,
               count(distinct CASE WHEN ACXCORPCODE='172' AND ACXCREDIT = 0 THEN NULL ELSE receiptid END) BILLCNT,
               0 
        from ax.retailtransactiontable 
        where ENTRYSTATUS = 0 
          and BUSINESSDATE between ? and ?
        group by acxcorpcode
        union all
        Select ISHEADER = 2,
               PAYMENTCODE,
               'HEALINGCARD-' + PAYMENTTYPE,
               sum(TRANSAMT) Amount,
               0, 0, 0, 0, 0 
        from HEALING_CARD_TRANSACTION 
        where ACTIONID in (0,1)
          and cast(TRANSACTIONDATE as date) between ? and ?
        group by PAYMENTCODE, PAYMENTTYPE
        union all
        Select ISHEADER = 4,
               0,
               'OMS CASH COLLECTION',
               isnull(SUM(COLLECTEDAMT),0) as COLLECTEDAMT,
               0, 0, 0, 0, 0  
        from ax.ACXSETTLEMENTDETAILS
        where cast(SETTLEMENTDATE as date) between ? and ?
        union all                    
        select ISHEADER = 5,
               tendertype,
               'IP COLLECTION',
               isnull(SUM(AMOUNTTENDERED), 0) as COLLECTEDAMT,
               0, 0, 0, 0, 0 
        from ax.retailtransactionpaymenttrans 
        where tendertype in (1,2)
          and receiptid like 'IP%'
          and BUSINESSDATE between ? and ? 
        group by tendertype
        """
        params = (
            from_date, to_date,   # For the first BETWEEN clause.
            from_date, to_date,   # For the subquery in the NOT IN clause.
            from_date, to_date,   # For the second block.
            from_date, to_date,   # For the corporate summary.
            from_date, to_date,   # For the HEALING_CARD_TRANSACTION date range.
            from_date, to_date,   # For the ACXSETTLEMENTDETAILS date range.
            from_date, to_date    # For the final BETWEEN clause.
        )
        cursor.execute(query, params)
        result = cursor.fetchall()
        return result, site_name
    finally:
        connection.close()

# =============================================================================
# Main Streamlit Application
# =============================================================================

def main():
    st.title("SALES SUMMARY REPORT")
    st.write("Upload an Excel (.xlsx) or text/CSV file containing one 5-digit Site ID.")

    if "zip_buffer" not in st.session_state:
        st.session_state.zip_buffer = None
    if "last_uploaded_file" not in st.session_state:
        st.session_state.last_uploaded_file = None
    if "disconnected_sites" not in st.session_state:
        st.session_state.disconnected_sites = {}

    # ------------------ Sidebar ------------------
    st.sidebar.header("Database Credentials")
    # Credentials are now loaded from .streamlit/secrets.toml
    username = st.secrets["USERNAME"]
    password = st.secrets["PASSWORD"]
    database = st.secrets["DATABASE"]

    st.sidebar.header("Server Settings")
    ip_series_choice = st.sidebar.radio("Select Server Series", ("16", "28"), key="ip_series")

    st.sidebar.header("Date Range")
    from_date_input = st.sidebar.date_input("From Date", value=datetime.today(), key="from_date")
    to_date_input = st.sidebar.date_input("To Date", value=datetime.today(), key="to_date")
    
    st.markdown(
        """
        <style>
        .fixed-container {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #f8f9fa;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 8px;
            z-index: 1000;
            max-width: 300px;
        }
        .fixed-container p {
            margin: 0;
            padding: 2px 0;
            font-size: 0.9em;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    
    # ------------------ Main Content ------------------
    uploaded_file = st.file_uploader("Upload Site IDs file", type=["xlsx", "txt", "csv"])
    
    if uploaded_file is not None:
        if st.session_state.last_uploaded_file != uploaded_file:
            st.session_state.zip_buffer = None
            st.session_state.disconnected_sites = {}
            st.session_state.last_uploaded_file = uploaded_file

        if uploaded_file.name.endswith('.xlsx'):
            try:
                df = pd.read_excel(uploaded_file)
                if any(col.lower() == "siteid" for col in df.columns):
                    df.columns = [col.lower() for col in df.columns]
                    site_ids = df["siteid"].astype(str).tolist()
                else:
                    st.error("The uploaded Excel file must contain a column named 'siteid'.")
                    site_ids = []
            except Exception as e:
                st.error(f"Error reading Excel file: {e}")
                site_ids = []
        else:
            try:
                file_bytes = uploaded_file.read()
                file_text = file_bytes.decode("utf-8")
                if "," in file_text:
                    site_ids = [s.strip() for s in file_text.split(",") if s.strip()]
                else:
                    site_ids = [s.strip() for s in file_text.splitlines() if s.strip()]
            except Exception as e:
                st.error(f"Error reading file: {e}")
                site_ids = []
        
        valid_site_ids = [sid for sid in site_ids if sid.isdigit() and len(sid) == 5]
        st.write(f"Found {len(valid_site_ids)} valid site IDs.")
        
        if st.button("Generate Reports"):
            if valid_site_ids:
                st.info("Validating credentials with test connection...")
                try:
                    test_conn = connect_to_database(valid_site_ids[0], username, password, database, ip_series_choice)
                    if not test_conn:
                        raise Exception("Test connection failed.")
                    test_conn.close()
                    st.success("Credentials validated successfully.")
                except Exception as e:
                    st.error(f"Invalid credentials: {e}")
                    st.stop()
            
            progress_placeholder = st.empty()
            successful_reports = {}
            disconnected_sites = {}
            
            for i, sid in enumerate(valid_site_ids, start=1):
                progress_placeholder.text(f"Processing site {sid} ({i}/{len(valid_site_ids)})...")
                try:
                    from_date_str = from_date_input.strftime("%Y-%m-%d")
                    to_date_str = to_date_input.strftime("%Y-%m-%d")
                    result, site_name = get_report_data(sid, from_date_str, to_date_str, username, password, database, ip_series_choice)
                    if result:
                        report_text = format_report(result, sid, site_name, from_date_str, to_date_str)
                        successful_reports[sid] = report_text
                    else:
                        successful_reports[sid] = "No data found."
                except Exception as e:
                    disconnected_sites[sid] = f"Error occurred: {e}"
            
            progress_placeholder.text("All sites processed.")
            st.session_state.disconnected_sites = disconnected_sites
            
            if successful_reports:
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                    for sid, report in successful_reports.items():
                        zip_file.writestr(f"{sid}.txt", report)
                zip_buffer.seek(0)
                st.session_state.zip_buffer = zip_buffer
            else:
                st.error("No successful reports to save.")

    if st.session_state.zip_buffer is not None or st.session_state.disconnected_sites:
        with st.container():
            st.markdown('<div class="fixed-container">', unsafe_allow_html=True)
            if st.session_state.zip_buffer is not None:
                st.download_button("Download Connected Site Reports (ZIP)", st.session_state.zip_buffer, file_name="SiteReports.zip", key="download_zip")
            if st.session_state.disconnected_sites:
                st.markdown("<p><strong>Disconnected Sites:</strong></p>", unsafe_allow_html=True)
                for sid, err in st.session_state.disconnected_sites.items():
                    st.markdown(f"<p>{sid}: {err}</p>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
