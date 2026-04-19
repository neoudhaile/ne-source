-- Rename company-level contact columns so owner_email / owner_phone and
-- company_email / company_phone are visually distinct throughout the stack.
ALTER TABLE smb_leads RENAME COLUMN email TO company_email;
ALTER TABLE smb_leads RENAME COLUMN phone TO company_phone;
