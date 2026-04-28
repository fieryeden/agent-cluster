<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LexCapital | Litigation Finance Investor Platform</title>
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700;900&family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{--navy:#0a1628;--navy-light:#0f1f3a;--navy-mid:#142a4a;--gold:#c9a961;--gold-light:#dfc28a;--gold-dark:#a88a3e;--white:#f0ece2;--gray:#8a9bb5;--gray-light:#c4cfdf;--success:#2ecc71;--warning:#e67e22;--danger:#e74c3c}
html{scroll-behavior:smooth;scroll-padding-top:80px}
body{font-family:'Inter',sans-serif;background:var(--navy);color:var(--white);line-height:1.6;overflow-x:hidden}
h1,h2,h3,h4{font-family:'Playfair Display',serif}
a{text-decoration:none;color:inherit}
.container{max-width:1200px;margin:0 auto;padding:0 24px}

/* NAV */
nav{position:fixed;top:0;left:0;right:0;z-index:1000;background:rgba(10,22,40,0.92);backdrop-filter:blur(16px);border-bottom:1px solid rgba(201,169,97,0.15);transition:all .3s}
nav.scrolled{background:rgba(10,22,40,0.98);box-shadow:0 4px 30px rgba(0,0,0,0.4)}
.nav-inner{display:flex;align-items:center;justify-content:space-between;height:72px;max-width:1200px;margin:0 auto;padding:0 24px}
.nav-logo{font-family:'Playfair Display',serif;font-size:1.5rem;font-weight:700;color:var(--gold);letter-spacing:1px}
.nav-logo span{color:var(--white);font-weight:400}
.nav-links{display:flex;gap:32px;list-style:none}
.nav-links a{font-size:.875rem;font-weight:500;color:var(--gray-light);letter-spacing:.5px;transition:color .3s;position:relative}
.nav-links a:hover,.nav-links a.active{color:var(--gold)}
.nav-links a::after{content:'';position:absolute;bottom:-4px;left:0;width:0;height:2px;background:var(--gold);transition:width .3s}
.nav-links a:hover::after,.nav-links a.active::after{width:100%}
.hamburger{display:none;flex-direction:column;gap:5px;cursor:pointer;background:none;border:none;padding:4px}
.hamburger span{width:24px;height:2px;background:var(--gold);transition:all .3s}
.mobile-menu{display:none;position:fixed;top:72px;left:0;right:0;background:rgba(10,22,40,0.98);backdrop-filter:blur(16px);padding:24px;border-bottom:1px solid rgba(201,169,97,0.15)}
.mobile-menu.open{display:block}
.mobile-menu a{display:block;padding:14px 0;font-size:1rem;color:var(--gray-light);border-bottom:1px solid rgba(201,169,97,0.08);transition:color .3s}
.mobile-menu a:hover{color:var(--gold)}

/* HERO */
.hero{min-height:100vh;display:flex;align-items:center;position:relative;overflow:hidden;padding-top:72px}
.hero-bg{position:absolute;inset:0;background:radial-gradient(ellipse at 20% 50%,rgba(201,169,97,0.06) 0%,transparent 60%),radial-gradient(ellipse at 80% 20%,rgba(201,169,97,0.04) 0%,transparent 50%)}
.hero-grid{position:absolute;inset:0;background-image:linear-gradient(rgba(201,169,97,0.03) 1px,transparent 