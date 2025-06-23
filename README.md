# جمع‌آوری کانفیگ‌های V2Ray 🌐

![بنر پروژه](https://via.placeholder.com/1200x300.png?text=V2Ray+Configs+Collector)

یک پروژه پیشرفته و قدرتمند برای جمع‌آوری خودکار کانفیگ‌های V2Ray از بیش از **1067 کانال تلگرامی** و منابع عمومی GitHub. این پروژه با هدف ارائه کانفیگ‌های به‌روز، سریع و امن برای دسترسی ناشناس به اینترنت طراحی شده است و کانفیگ‌ها را بر اساس **پروتکل**، **کشور** و **دیتاسنتر** دسته‌بندی می‌کند. 🚀

> **وضعیت پروژه**: در حال حاضر در فاز **بتا** است و به طور مداوم در حال بهبود و به‌روزرسانی است.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GitHub Actions](https://img.shields.io/github/actions/workflow/status/PlanAsli/configs-collector-v2ray/main.yml?branch=main)](https://github.com/PlanAsli/configs-collector-v2ray/actions)
[![Issues](https://img.shields.io/github/issues/PlanAsli/configs-collector-v2ray)](https://github.com/PlanAsli/configs-collector-v2ray/issues)
[![Stars](https://img.shields.io/github/stars/PlanAsli/configs-collector-v2ray)](https://github.com/PlanAsli/configs-collector-v2ray/stargazers)

---

## ✨ ویژگی‌های کلیدی

- **جمع‌آوری گسترده**: جمع‌آوری کانفیگ‌ها از **1067+ کانال تلگرامی** و مخازن GitHub به صورت خودکار.
- **دسته‌بندی پیشرفته**:
  - بر اساس **پروتکل** (VLESS، VMess، Shadowsocks، Trojan و غیره)
  - بر اساس **کشور** (بیش از 70 کشور، از امارات تا آفریقای جنوبی)
  - بر اساس **شبکه و دیتاسنتر** (ws، gRPC، reality و غیره)
- **به‌روزرسانی مداوم**: کانفیگ‌ها هر **30 دقیقه** به‌روزرسانی می‌شوند.
- **کانفیگ‌های میکس**: ارائه لینک‌های ترکیبی برای استفاده آسان‌تر.
- **آمار کانال‌ها**: گزارش‌های جامع از تعداد کانفیگ‌های جمع‌آوری‌شده از هر کانال در `Logs/channel_stats.json`.
- **حذف خودکار کانال‌های نامعتبر**: کانال‌های غیرفعال به صورت خودکار از لیست حذف می‌شوند.

---

## 🚀 شروع سریع

1. **پیش‌نیاز یک**:
   - یک کلاینت V2Ray مانند **V2RayCli** (اندروید)، **Nekoray** (ویندوز)، یا **Streisand** (iOS).
2. **لینک اشتراک را کپی کنید**:
   - برای پروتکل‌های خاص: به لینک‌های زیر در بخش پروتکل‌ها مراجعه کنید.
   - برای کانفیگ‌های میکس: به لینک‌های زیر در بخش کانفیگ‌های میکس مراجعه کنید.
   - برای کانفیگ‌های دسته‌بندی‌شده بر اساس کشور: به لینک‌های زیر در بخش دسته‌بندی بر اساس کشور مراجعه کنید.
3. **لینک را در کلاینت V2Ray وارد کنید**:
   - در کلاینت خود، به بخش **Subscription** بروید و لینک را جای‌گذاری کنید.
   - برای مثال، در V2RayCli: روی "+" کلیک کنید و گزینه "Import Config from Clipboard" را انتخاب کنید.
4. **بخ‌روزرسانی منظم**:
   - برای بر اساس، اشتراک خود را هر چند ساعت یک‌بار بخ‌روزرسانی کنید.

---

## 📖 لینک‌های کانفیگ

### پروتکل‌ها

| پروتکل        | لینک                                                                 |
|---------------|----------------------------------------------------------------------|
| VLESS         | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/protocols/vless.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/protocols/vless.txt) |
| VMess         | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/protocols/vmess.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/protocols/vmess.txt) |
| Shadowsocks   | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/protocols/shadowsocks.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/protocols/shadowsocks.txt) |
| Trojan        | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/protocols/trojan.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/protocols/trojan.txt) |

### شبکه‌ها

| شبکه          | لینک                                                                 |
|---------------|----------------------------------------------------------------------|
| WebSocket (ws)| [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/ws.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/ws.txt) |
| gRPC          | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/grpc.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/grpc.txt) |
| HTTP          | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/http.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/http.txt) |
| HTTP Upgrade  | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/httpupgrade.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/httpupgrade.txt) |
| kCP           | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/kcp.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/kcp.txt) |
| Reality       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/reality.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/reality.txt) |
| TCP           | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/tcp.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/tcp.txt) |
| xHTTP         | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/xhttp.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/xhttp.txt) |
| HTTP/2 (h2)   | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/h2.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/h2.txt) |
| Raw           | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/raw.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/networks/raw.txt) |

### کانفیگ‌های میکس

| فایل          | لینک                                                                 |
|---------------|----------------------------------------------------------------------|
| Mixed 1       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_1.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_1.txt) |
| Mixed 2       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_2.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_2.txt) |
| Mixed 3       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_3.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_3.txt) |
| Mixed 4       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_4.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_4.txt) |
| Mixed 5       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_5.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_5.txt) |
| Mixed 6       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_6.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_6.txt) |
| Mixed 7       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_7.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_7.txt) |
| Mixed 8       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_8.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_8.txt) |
| Mixed 9       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_9.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_9.txt) |
| Mixed 10      | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_10.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/splitted/mixed_10.txt) |

### دسته‌بندی بر اساس کشور

| کشور          | لینک                                                                 |
|---------------|----------------------------------------------------------------------|
| امارات متحده عربی (AE) | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AE.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AE.txt) |
| آلبانی (AL)   | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AL.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AL.txt) |
| ارمنستان (AM) | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AM.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AM.txt) |
| آرژانتین (AR) | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AR.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AR.txt) |
| اتریش (AT)    | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AT.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AT.txt) |
| استرالیا (AU) | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AU.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AU.txt) |
| آذربایجان (AZ)| [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AZ.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/AZ.txt) |
| بوسنی و هرزگوین (BA) | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BA.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BA.txt) |
| بلژیک (BE)    | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BE.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BE.txt) |
| بلغارستان (BG)| [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BG.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BG.txt) |
| بحرین (BH)    | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BH.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BH.txt) |
| بولیوی (BO)   | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BO.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BO.txt) |
| برزیل (BR)    | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BR.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BR.txt) |
| بلیز (BZ)     | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BZ.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/BZ.txt) |
| کانادا (CA)    | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CA.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CA.txt) |
| سوئیس (CH)    | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CH.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CH.txt) |
| شیلی (CL)     | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CL.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CL.txt) |
| چین (CN)      | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CN.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CN.txt) |
| کلمبیا (CO)   | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CO.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CO.txt) |
| کاستاریکا (CR)| [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CR.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CR.txt) |
| قبرس (CY)     | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CY.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CY.txt) |
| چک (CZ)       | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CZ.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/CZ.txt) |
| آلمان (DE)    | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/DE.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/DE.txt) |
| دانمارک (DK)  | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/DK.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/DK.txt) |
| اکوادور (EC)  | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/EC.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/EC.txt) |
| استونی (EE)   | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/EE.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/EE.txt) |
| اسپانیا (ES)  | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/ES.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/ES.txt) |
| فنلاند (FI)   | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/FI.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/FI.txt) |
| فرانسه (FR)    | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/FR.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/FR.txt) |
| بریتانیا (GB) | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/GB.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/GB.txt) |
| یونان (GR)    | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/GR.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/GR.txt) |
| گواتمالا (GT) | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/GT.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/GT.txt) |
| هنگ‌کنگ (HK)  | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/HK.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/HK.txt) |
| کرواسی (HR)   | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/HR.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/HR.txt) |
| مجارستان (HU) | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/HU.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/HU.txt) |
| اندونزی (ID)  | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/ID.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/ID.txt) |
| ایرلند (IE)   | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/IE.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/IE.txt) |
| اسرائیل (IL)  | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/IL.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/IL.txt) |
| هند (IN)      | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/IN.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/IN.txt) |
| ایران (IR)    | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/IR.txt](https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/IR.txt) |
| ایسلند (IS)   | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/IU.txt) |
| ایتالیا (IT)  | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/IT.txt) |
| اردن (JO)     | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/JO.txt) |
| ژاپن (JP)     | [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/JP.txt) |
| کره جنوبی (KR)| [https://github.com/PlanAsli/configs-collector-v2ray/blob/main/sub/countries/KR.txt) |
| قزاقستان (KZ) | [https://github.com/PlanAsli/configs-collect-or-v2ray/blob/main/sub/countries/KZ.txt) |
| لیتوانی (LT)    | [https://github.com/PlanAsli/configs-col-lector-v2ray/blob/main/sub/countries/LT.txt) |
| لوکزامبورگ (LU) | [https://github.com/PlanAsli/configs-c-ollector-v2ray/blob/main/sub/countries/LU.txt) |
| لتونی (LV)     | [https://github.com/PlanAsli/configs-collect-or-v2ray/blob/main/sub/countries/LV.txt) |
| مولداوی (MD)    | [https://github.com/PlanAsli/configs-co-llector-v2ray/blo/main/sub/countries/MD.txt) |
| مقدونیه (MK)    | [https://github.com/PlanAsli/configs-c-o-llector-v2ray/blob/main/sub/countries/MK.txt) |
| میانمار (MM)    | [https://github.com/PlanAsli/configs-c-o-llector-v2ray/blob/main/sub/countries/MM.txt) |
| مغولستان (MN)    | [https://github.com/PlanAsli/configs-c-o-llector-v2ray/blob/main/sub/countries/MN.txt) |
| ماکائو (MO)      | [https://github.com/PlanAsli/configs-c-o-llector-v2ray/blob/main/sub/countries/MO.txt) |
| مالت (MT)        | [https://github.com/PlanAsli/configs-c-o-llector-v2ray/blob/main/sub/countries/MT.txt) |
| موریس (MU)       | [https://github.com/PlanAsli/configs-c-o-llector-v2ray/blob/main/sub/countries/MU.txt) |
| مکزیک (MX)       | [https://github.com/PlanAsli/configs-c-o-l-lector-v2ray/blob/main/sub/countries/MX.txt) |
| مالزی (MY)       | [https://github.com/PlanAsli/configs-c-o-l-l-ector-v2ray/blob/main/sub/countries/MY.txt) |
| نیجریه (NG)      | [https://github.com/PlanAsli/configs-c-o-l-l-e-v2ray/blob/main/sub/countries/NG.txt) |
| هلند (NL)       | [https://github.com/PlanAsli/configs-c-o-l-l-e-c-v2ray/blob/main/sub/countries/NL.txt) |
| نروژ (NO)        | [https://github.com/PlanAsli/configs-c-o-l-l-e-c-t-v2ray/blob/main/sub/countries/NO.txt) |
| نیوزیلند (NZ)    | [https://github.com/PlanAsli/configs-c-o-l-l-e-c-t-o-v2ray/blob/main/sub/untries/NZ.txt) |
| عمان (OM)        | [https://github.com/PlanAsli/configs-c-o-l-l-e-c-t-o-r-v2ray/blob/main/sub/countries/OM.txt) |
| پاناما (PA)     | [https://github.com/PlanAsli/configs-c-o-l-l-e-c-t-o-r-v2ray/blob/main/sub/countries/PA.txt) |
| پرو (PE)        | [https://github.com/PlanAsli/configs-co-o-l-l-l-e-c-t-o-r-v2ray/blob/main/sub/countries/PE.txt) |
| فیلیپین (PH)     | [https://github.com/PlanAsli/configs-co-o-l-l-l-e-c-t-o-r-v2ray/b/main/sub/countries/PH.txt) |
| پاکستان (PK)    | [https://github.com/PlanAsli/configs-co-o-l-l-l-l-e-c-t-o-r-v2ray/blob/main/sub/countries/PK.txt) |
| لهستان (PL)     | [https://github.com/PlanAsli/configs-c-co-o-l-l-l-l-e-c-t-o-r-v2ray/blob/main/sub/countries/PL.txt) |
| پورتوریکو (PR) | [https://github.com/PlanAsli/configs-c-c-o-o-l-l-l-l-e-c-t-o-r-v2ray/blob/main/r/PR.txt) |
| پرتغال (PT)    | [https://github.com/pPlanAsli/configs-c-c-o-o-l-l-l-e-c-t-o-r-v2ray/blo/main/sub/countries/PT.txt) |
| پاراگوئه (PY)   | [https://github.com/pPlanAsli/pconfigs-c-c-o-l-l-l-l-e-c-t-o-r-v2ray/blo/r/PY.txt) |
| رومانی (RO)     | [https://github.com/pPlanAsli/p/configs-c-c-c-o-l-l-l-l-e-c-t-o-r-v2r/b/r/RO.txt) |
| صربستان (RS)    | [https://github.com/PlanAsli/p/pconfigs-c-c-c-o-l-l-l-l-e-c-t-o-r-v/2-r-v2ray/r/RS.txt) |
| روسیه (RU)      | [https://github.com/PlanAsli/p/p-l-l-l-e-c-c-c-o-l-l-l-l-e-c-t-o-r/2ray/r/RU.txt) |
| عربستان (SA)    | [https://github.com/pPlanAsli/p/p-l-l-l-e-c-t-c-c-o-l-l-l-l-e-c-t-o-r/2-ray/r/SA.txt) |
| سیشل (SC)       | [https://github.com/p/PlanAsli/p/p-l-l-e-c-t-o-c-c-o-l-l-l-l-e-c-t-o-r/2-ray/r/SC.txt) |
| سوئد (SE)       | [https://github.com/p/pPlanAsli/p/p-l-l-e-c-t-o-r-c-c-o-l-l-l-l-e-c-t/2-ray/r/SE.txt) |
| سنگاپور (SG)    | [https://github.com/p/pPlanAsli/p/p-l-l-l-e-c-t-o-r-v-c-o-l-l-l-l-e-c-t-o/2-ray/r/SG.txt) |
| اسلوونی (SI)    | [https://github.com/p/p/PlanAsli/p/p-l-l-l-l-e-c-t-o-r-v-t-c-o-l-l-l-l-e-c-t-o/2-ray/r/SI.txt) |
| اسلواک (SK)     | [https://github.com/p/p/PlanAsli/p/p/l-l-l-l-e-c-t-o-r-v-e-c-t-c-o-l-l-l-l-l-e-c-t-o/2-ray/r/SK.txt) |
| تایلند (TH)     | [https://t/PlanAsli/p/p/p-l-l-l-l-e-c-t-o-r-v-e-c-t-o-r-c-o-l-l-l-l-e-c-t-o/2-ray/r/TH.txt) |
| ترکیه (TR)      | [https://github.com/PlanAsli/p/p/p/p-l-l-l-l-e-c-t-o-r-v-e-c-t-o-r-v-c-o-l-l-l-l-e-c-t-o/2-ray/r-t/TR.txt) |
| تایوان (TW)     | [https://github.com/p/PlanAsli/p/p/p-l-l-l-l-l-e-c-t-o-r-v-e-c-t-o-r-v-t-c-o-l-l-l-l-e-c-t-o/2-ray/r-t/r/TW.txt) |
| اوکراین (UA)    | [http://github.com/p/PlanAsli/p/p/p-l-l-l-l-l-e-c-t-o-r-v-e-c-t-o-r-v-t-c-o-l-l-l-l-e-c-t-o/2-ray/r-t/UA.txt) |
| ایالات متحده (US)| [https://github.com/p/p/PlanAsli/p/p-l-l-l-l-l-e-c-t-o-r-v-e-c-t-o-r-v-t-v-c-o-l-l-l-l-e-c-t-o/2-ray/r-t/US.txt) |
| جزایر ویرجین بریتانیاVG) | [https://github.com/p/p/PlanAsli/l-l-l-l-l-e-c-t-o-r-v-e-c-t-o-r-v-t-v-t-c-o-l-l-l-l-e-c-t-o/2-ray/r-t/VG.txt) |
| ویتنام (VN)     | [https://github.com/p/p/PlanAsli/l-l-l-l-l-e-c-t-o-r-v-e-c-t-o-r-v-t/v-t-c-o-t-l-l-l-l-e-c-t-o/2-ray/r-t/VN.txt) |
| آفریقای جنوبی (ZA)| [https://github.com/p/p/p/PlanAsli/l-l-l-l-l-e-c-t-o-r-v-e-c-t-o-r-v-t/v-t/v-t-c-o-l-l-t-l-l-e-c-t-o/2-ray/r-t/ZA.txt) |

---

## 🤝 مشارکت در پروژه

ما از مشارکت شما استقبال می‌کنیم! برای کمک به بهبود پروژه:

1. **گزارش باگ یا پیشنهاد ویژگی**:: از بخش **Issues** section استفاده کنید.
2. **ضافه کردن کانال جدید**: اگر کانال تلگرامی جدیدی می‌شناسید که کانفیگ‌های V2Ray ارائه می‌دهد، در بخش Issues اطلاع دهید.
3. **رسال Pull Request**:
   - مخزن را Fork کنید.
   - تغییرات را در یک شاخه جدید اعمال کنید:
     ```bash
     git checkout -b feature/your-feature
     ```
   - تغییرات را کامیت و Push کنید:
     ```bash
     git commit -m "اضافه کردن ویژگی جدید"
     ```
     - یک درخواست Pull Request باز کنید.

جزییات بیشتر در [CONTRIBUTING](https://github.com/PlanAsli/configs-c-o-l-l-e-c-t-o-r-v-e-c-t-o-r-t/2-ray/r-t/CONTRIBUTING.md).

---

## 📊 آمار و گزارش‌ها

- **telegram_channels.json**: Contains a dynamically updated list of Telegram channels.
- **Logs/invalid_channels.txt**: Lists channels that are no longer valid.
- **Logs/channel_stats.json**: Includes statistics on the number of configs per channel by protocol and total count, and score.

---

## ⚠️ نکات مهم

- این پروژه در فاز **بتا** است و ممکن است باگ‌هایی داشته باشد. لطفاً مشکلات را گزارش دهید.
- برخی برخی کانال‌ها ممکن است کانفیگ‌های نامعتبر ارائه دهند. 
- برای بهترین عملکرد، از کلاینت‌های به‌روز مانند **Nekoray** یا **V2RayCli** استفاده کنید.

---

## 📜 License

این پروژه تحت [MIT License](https://github.com/PlanAsli/configs-c-o-l-l-e-c-t-o-r-v-e-c-t-o-r-t/2-ray/r-t/L-t/CONTRIBUTING/License.txt) منتشر شده است.

---

## 🌟 Acknowledgments

- Thanks to all Telegram channels that provide free V2Ray configs.
- Gratitude to the open-source community for inspiration and support.
- Thank you for using and supporting this project! 😊

---

Built with love by **[PlanAsli](https://github.com/PlanAsli)**. Follow us on [Telegram](https://t.me/Your-channel-name) for updates!
