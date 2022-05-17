class CategoryAnonNotification < ApplicationMailer
  def notify(user, cat, did)
    @did = did 
    @uid = user.id
    @cat = cat

    mail(
      :to => user.email,
      :subject => "[#{Rails.application.name}] Your category posts have been anonymized"
    )
  end
end
